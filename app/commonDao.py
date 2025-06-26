import ast
from collections import defaultdict
import datetime
from decimal import Decimal
import json
import logging
from bson.errors import InvalidId
from dateutil import parser
from pprint import pformat
from typing import (
    Any,
    Dict,
    ForwardRef,
    Generator,
    List,
    Optional,
    Union,
    Tuple,
    get_origin,
    get_type_hints,
    Annotated,
    get_args,
)
from asyncio import gather
from datetime import datetime
from mongoz import EmbeddedDocument
from pydantic import Field
from app.baseModel import BaseDocument
from bson import ObjectId
from esmerald import AsyncDAOProtocol
from motor.motor_asyncio import AsyncIOMotorClient
import re
from app.utils import BadRequest
from config.settings import settings

logger = logging.getLogger(__name__)


class CommonDAO(AsyncDAOProtocol):
    db: Optional[str] = None

    def __init__(self, db: Optional[str] = None):
        self.db = db
        client = AsyncIOMotorClient(settings.mongo_uri)
        self.client = client
        self.database = self.client[self.db]
        collection = client["test_database"]["students"]
        self.collection = collection

    def qs(self):
        if not hasattr(self.model, "objects") or self.model.objects is None:
            raise RuntimeError(
                f"Model {self.model.__name__} does not have a valid 'objects' manager."
            )
        obj = self.model.objects.using(self.db)
        return obj

    def get_field_url(self, field_path: str) -> Optional[str]:
        parts = field_path.split(".")
        current_model = self.model
        current_field = None

        for i, part in enumerate(parts):
            current_field = current_model.model_fields.get(part)
            if current_field is None:
                return None

            is_last = i == len(parts) - 1

            # If not the last part, we must traverse
            if not is_last:
                if hasattr(current_field, "to"):
                    current_model = current_field.to
                elif self.is_embedded_field(current_field):
                    current_model = current_field.model
                else:
                    return None
            else:
                # Last part: check if it has a .url property
                return getattr(current_field, "url", None)

        return None

    async def search(
        self,
        params: Dict[str, Any] = None,
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        external_pipeline: Optional[List[Dict[str, Any]]] = None,
        is_total_count: bool = False,
        supported_operators: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, List[Dict[str, Any]]]:

        params = params or {}
        pipeline: List[Dict[str, Any]] = []
        projection_stage: Dict[str, Any] = {}
        combined_query = {}
        sort_criteria = {}
        skip_count = int(params.pop("skip", 0))
        limit_count = int(params.pop("pick", 0))
        group_by_field = params.pop("group_by", None)

        group_by_field = params.pop("group_by", None)

        # --- Parse sort fields ---
        sort_param = params.pop("sort", None)
        if sort_param:
            for field in sort_param.split(","):
                if field.startswith("-"):
                    # Descending sort
                    sort_criteria[field[1:]] = -1
                else:
                    # Ascending sort
                    sort_criteria[field] = 1

        (
            lookup_stages,
            base_filters,
            lookup_filters,
            unwind_paths,
            url_fields,
        ) = self._extract_lookups_from_params(self.model, params, projection)
        pipeline.extend(lookup_stages)

        if base_filters:
            combined_query.update(
                await self.query_builder(base_filters, supported_operators)
            )
        if lookup_filters:
            combined_query.update(
                await self.query_builder(lookup_filters, supported_operators)
            )
        if combined_query:
            pipeline.append({"$match": combined_query})

        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        if projection:
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = f"${item}"
                else:
                    field_path, alias = item
                    projection_stage[alias] = {
                        "$ifNull": [f"${field_path}", None]
                    }
        else:
            projection_stage = {
                field: f"${field}"
                for field in self.model.__annotations__.keys()
            }

        additional_values = {
            field_name: self.get_field_url(field_name)
            for field_name in url_fields
            if self.get_field_url(field_name)
        }

        if additional_values:
            self.add_extra_value_to_pipeline(additional_values, pipeline)

        if external_pipeline:
            pipeline.extend(external_pipeline)

        if projection_stage:
            pipeline.append({"$project": projection_stage})

        if group_by_field:
            group_stage = {
                "_id": f"${group_by_field}",
                "original_id": {"$first": "$_id"},
            }
            push_fields = defaultdict(list)
            first_fields = {}
            for alias, source in projection_stage.items():
                if alias == "_id":
                    continue
                root = source[1:].split(".")[0]
                (push_fields if "." in source else first_fields)[
                    root if "." in source else alias
                ].append((alias, f"${alias}"))

            for root, fields in push_fields.items():
                group_stage[root] = {"$addToSet": {k: v for k, v in fields}}
            for alias, fields in first_fields.items():
                group_stage[alias] = {"$first": fields[0][1]}

            pipeline.append({"$group": group_stage})
            pipeline.append(
                {
                    "$project": {
                        "_id": "$original_id",
                        group_by_field: "$_id",
                        **{
                            k: 1
                            for k in group_stage
                            if k not in {"_id", "original_id"}
                        },
                    }
                }
            )

        if group_by_field and sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        count_result = await self.aggregate(pipeline)
        filtered_data_count = len(count_result)
        if is_total_count:
            return filtered_data_count, []

        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        pipeline.append({"$project": {"_id": 0}})
        results = await self.aggregate(pipeline)

        return filtered_data_count, json.loads(
            json.dumps(results, default=str)
        )

    def _extract_lookups_from_params(
        self,
        model,
        params: Dict[str, Any],
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], set]:
        if not params and not projection:
            return [], {}, {}, set()

        lookups = {}
        base_params = {}
        lookup_params = {}
        unwind_paths = set()
        url_fields = []

        # --- Internal helper to build lookup chain ---
        def add_lookup_chain(field_path: str):
            current_path = ""
            for full_path, model_field in self._resolve_field_path(
                model, field_path
            ):
                if hasattr(model_field, "to"):  # ForeignKey relation
                    if full_path not in lookups:
                        collection = model_field.to.Meta.collection.name
                        lookups[full_path] = {
                            "from": collection,
                            "localField": full_path,
                            "foreignField": "_id",
                            "as": full_path,
                        }
                    unwind_paths.add(full_path)
                elif self.is_embedded_field(model_field):
                    unwind_paths.add(full_path)
                else:
                    # Ensure parent embedded fields are unwound
                    parent_path = ".".join(full_path.split(".")[:-1])
                    if parent_path:
                        unwind_paths.add(parent_path)

        # --- Parse and classify params ---
        for param_key, param_value in params.items():
            base_field_path = param_key.split("__")[0]
            if "." in base_field_path:
                add_lookup_chain(base_field_path)
                lookup_params[param_key] = param_value
                if self.get_field_url(base_field_path):
                    url_fields.append(base_field_path)
            else:
                base_params[param_key] = param_value
                if self.get_field_url(base_field_path):
                    url_fields.append(base_field_path)

        # --- Parse projections for additional lookups ---
        if projection:
            for projected_field in projection:
                field_path = (
                    projected_field[0]
                    if isinstance(projected_field, tuple)
                    else projected_field
                )
                if "." in field_path:
                    add_lookup_chain(field_path)
                    for i in range(1, len(field_path.split("."))):
                        unwind_paths.add(".".join(field_path.split(".")[:i]))
                if self.get_field_url(field_path):
                    url_fields.append(field_path)

        # --- Build lookup/unwind stages ---
        lookup_stages = []
        added_unwinds = set()

        for path in sorted(unwind_paths):
            if path not in lookups and path not in added_unwinds:
                lookup_stages.append(
                    {
                        "$unwind": {
                            "path": f"${path}",
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )
                added_unwinds.add(path)

        for full_path, lookup in lookups.items():
            lookup_stages.append({"$lookup": lookup})
            if full_path not in added_unwinds:
                lookup_stages.append(
                    {
                        "$unwind": {
                            "path": f"${full_path}",
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )
                added_unwinds.add(full_path)

        return (
            lookup_stages,
            base_params,
            lookup_params,
            unwind_paths,
            url_fields,
        )

    def build_lookup_pipeline(
        self, model: Any, projections: List[Union[str, Tuple[str, str]]]
    ) -> List[Dict[str, Any]]:
        if not projections:
            return []

        pipeline = []
        visited_paths = set()

        used_fields = set(
            path if isinstance(path, str) else path[0] for path in projections
        )

        for field_path in used_fields:
            for full_path, model_field in self._resolve_field_path(
                model, field_path
            ):
                if full_path in visited_paths:
                    continue
                visited_paths.add(full_path)

                if hasattr(model_field, "model"):
                    collection = (
                        model_field.model.Meta.collection._collection.name
                    )
                    pipeline.append(
                        {
                            "$lookup": {
                                "from": collection,
                                "localField": full_path,
                                "foreignField": "_id",
                                "as": full_path,
                            }
                        }
                    )
                    pipeline.append(
                        {
                            "$unwind": {
                                "path": f"${full_path}",
                                "preserveNullAndEmptyArrays": True,
                            }
                        }
                    )

                elif self.is_embedded_field(model_field):
                    pipeline.append(
                        {
                            "$unwind": {
                                "path": f"${full_path}",
                                "preserveNullAndEmptyArrays": True,
                            }
                        }
                    )

        # === Add Choice Label Fields ===
        add_fields = self.collect_choice_label_fields(model, used_fields)
        if add_fields:
            pipeline.append({"$addFields": add_fields})

        return pipeline

    def convert_to_serializable(self, doc: dict) -> dict:
        def convert(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, Decimal):
                return float(value)
            elif isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert(v) for v in value]
            return value  # int, float, bool, None are untouched

        return convert(doc)

    async def query_builder(
        self,
        filters: Dict[str, Any],
        supported_operators: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        query = {}
        OPERATORS = settings.query_param_operators

        if supported_operators:
            for operator in supported_operators:
                if operator not in OPERATORS:
                    raise BadRequest(
                        f"Unsupported operator provided in API: {operator}"
                    )

            OPERATORS = {op: OPERATORS[op] for op in supported_operators}

        for key, value in filters.items():
            # Default to "eq"
            field = key
            operator = "eq"

            # Check for operator suffix
            skip_operator = False
            for op in OPERATORS:
                if key.endswith(f"__{op}"):
                    operator = op
                    field = key.rsplit(f"__{op}", 1)[0]
                    skip_operator = True
                    break

            if skip_operator is False and key:
                continue

            mongo_op = OPERATORS[operator]

            if operator == "in":
                value = self.__parse_str_to_list(value)

                if not isinstance(value, list):
                    raise BadRequest(
                        f"'in' operator requires a list for field '{field}'"
                    )

                parsed_values = [
                    self.__parse_values(field, val) for val in value
                ]
                query[field] = {mongo_op: parsed_values}

            elif operator == "be":
                value = self.__parse_str_to_list(value)

                if isinstance(value, list) and len(value) == 2:
                    try:
                        value = [self.__auto_cast(v) for v in value]
                    except Exception as e:
                        raise BadRequest(
                            f"Failed to parse 'between' values: {value}"
                        ) from e

                    query[field] = {"$gte": value[0], "$lte": value[1]}
                else:
                    raise BadRequest(
                        f"Invalid value for 'between' operator on field '{field}': {value}"
                    )

            # Handle regex-based operators
            elif operator in {"sw", "ew", "co", "ico", "ieq"}:
                escaped_value = re.escape(value)
                if operator == "sw":
                    regex = f"^{escaped_value}"
                elif operator == "ew":
                    regex = f"{escaped_value}$"
                elif operator in {"co", "ico"}:
                    regex = f".*{escaped_value}.*"
                elif operator == "ieq":
                    regex = f"^{escaped_value}$"

                flags = re.I if operator in {"ico", "ieq"} else 0
                query[field] = {"$regex": re.compile(regex, flags)}

            elif operator in ["lt", "lte", "gt", "gte", "ne"]:
                query[field] = {mongo_op: self.__parse_values(field, value)}

            else:
                query[field] = {mongo_op: self.__parse_values(field, value)}

        return query

    def __parse_values(self, key: str, value: str) -> Any:
        """Parse values into appropriate types for MongoDB queries."""
        if not isinstance(value, str):
            return value

        if key.endswith("_id"):
            try:
                return ObjectId(value)
            except (InvalidId, TypeError):
                logger.info(f"Invalid ObjectId for '{key}': {value}")

        if value.isdigit():
            return int(value)

        try:
            return parser.isoparse(value)
        except (ValueError, TypeError):
            pass

        return value

    def __parse_str_to_list(self, value: Union[str, list]) -> list:
        if isinstance(value, list):
            return value

        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                try:
                    return ast.literal_eval(value)
                except Exception:
                    value = value.strip("[]").split(",")
            else:
                value = value.split(",")

            return [v.strip() for v in value]

        raise ValueError(
            f"Expected a list or string that can be parsed as list, got {type(value)}"
        )

    def __auto_cast(self, val: str):
        val = val.strip()
        # Try datetime formats
        date_formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue

        # If int
        try:
            return int(val)
        except ValueError:
            pass

        # if float
        try:
            return float(val)
        except ValueError:
            pass

        return val

    def extract_field_and_operator(
        self, key: str, operators: dict
    ) -> Optional[tuple[str, str]]:
        """
        Extract field and operator from a key. Return None if no valid operator is found.
        """
        for op in operators:
            if key.endswith(f"__{op}"):
                field = key[: -len(f"__{op}")]
                return field, op
        # If no __operator suffix, default to 'eq' only if operator is supported
        if "__" not in key and "eq" in operators:
            return key, "eq"
        return None

    def add_extra_value_to_pipeline(
        self, additional_value: Dict[str, str], pipeline: List[Dict[str, Any]]
    ):
        add_fields_stage = [
            {
                "$addFields": {
                    field_name: {
                        "$concat": [
                            base_path,
                            (
                                f"${'.'.join(field_name.split('.')[:-1])}.{field_name.split('.')[-1]}"
                                if "." in field_name
                                else f"${field_name}"
                            ),
                        ]
                    }
                }
            }
            for field_name, base_path in additional_value.items()
        ]
        pipeline.extend(add_fields_stage)

    def is_embedded_field(self, model_field) -> bool:
        ann = getattr(model_field, "annotation", None)

        # Unwrap Union (e.g., Optional[...] or Union[List[Model], None])
        if get_origin(ann) is Union:
            args = get_args(ann)
            ann = next((arg for arg in args if arg is not type(None)), None)

        # Unwrap List[...] if needed
        if get_origin(ann) in (list, List):
            ann = get_args(ann)[0]

        return hasattr(ann, "model_fields")

    def collect_choice_label_fields(self, model, used_fields: set) -> dict:
        add_fields = {}

        for field_path in used_fields:
            resolved_fields = self._resolve_field_path(model, field_path)

            for full_path, model_field in resolved_fields:
                # Skip if field is relational (has model or is embedded)
                if hasattr(model_field, "model") or self.is_embedded_field(
                    model_field
                ):
                    continue

                # If the field has choices, generate the switch expression
                choices = getattr(model_field, "choices", None)
                if choices:
                    add_fields[full_path] = {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": [f"${full_path}", code]},
                                    "then": label,
                                }
                                for code, label in choices
                            ],
                            "default": f"${full_path}",
                        }
                    }
                break  # Stop after finding the first non-relational field

        return add_fields

    def _resolve_field_path(
        self, model: Any, field_path: str
    ) -> Generator[Tuple[str, Any], None, None]:
        """
        Yield (full_path, model_field) tuples walking the path from the model.
        Handles Unions, lists, embedded docs, ForeignKey relations.
        Stops if a field is missing.
        """
        parts = field_path.split(".")
        current_model = model
        current_path = ""

        for part in parts:
            origin = get_origin(current_model)
            if origin is Union:
                args = get_args(current_model)
                non_none_args = [arg for arg in args if arg is not type(None)]
                if len(non_none_args) == 1:
                    current_model = non_none_args[0]
                    origin = get_origin(current_model)
            if origin is list:
                current_model = get_args(current_model)[0]

            model_fields = getattr(current_model, "model_fields", {})
            model_field = model_fields.get(part)
            if not model_field:
                break

            full_path = f"{current_path}.{part}" if current_path else part
            yield full_path, model_field

            if hasattr(model_field, "model"):  # Foreign key
                current_model = model_field.model
            elif self.is_embedded_field(
                model_field
            ):  # Embedded or list[embedded]
                ann = getattr(model_field, "annotation", None)
                current_model = (
                    get_args(ann)[0] if get_origin(ann) is list else ann
                )
            else:
                break

            current_path = full_path

    async def aggregate(self, pipeline: List) -> List:
        """This method is use to aggregate the pipeline from DB."""

        result = await self.model.objects.using(
            self.db
        )._collection._async_aggregate(pipeline)
        return list(result)
