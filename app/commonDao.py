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

    async def search(
        self,
        params: Dict[str, Any] = None,
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: Optional[Dict[str, int]] = None,
        unwind_fields: Optional[List[str]] = None,
        additional_value: Optional[Dict[str, str]] = None,
        external_pipeline: Optional[List[Dict[str, Any]]] = None,
        is_total_count: bool = False,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Use Example:
        search(
            params=q,
            is_total_count=True,
            projection=[
                "name",
                "std",
                "roll_no",
                "obtained_pct",
                "is_pass",
                "section",
                "mobile_number",
                ("school_id.university_id.body.body_name", "body_name"),
                (
                    "school_id.university_id.body.country_id.country_name",
                    "body_country_name",
                ),
                ("address.village", "village"),
                ("address.country_id.country_name", "country_name"),
                (
                    "address.country_id.continent_id.continent_name",
                    "continent_name",
                ),
                ("school_id.name", "school_name"),
                ("school_id.board", "school_board"),
                ("school_id.university_id.un_name", "university_name"),
            ],
            additional_value={"school_id.name": "TESTtt"},
            external_pipeline=external_pipeline,
        )
        http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.country_name__sw=I
        """

        # --- Safe default handling ---
        params = params or {}
        unwind_fields = unwind_fields or []
        pipeline: List[Dict[str, Any]] = []
        projection_stage: Dict[str, Any] = {}
        sort_criteria: Dict[str, int] = {}

        # --- Pagination ---
        skip_count = int(params.pop("skip", 0))
        limit_count = int(params.pop("pick", 0))
        group_by_field = params.pop("group_by", None)

        # --- Sorting ---
        sort_param = params.pop("sort", None)
        if sort_param:
            for field in sort_param.split(","):
                direction = 1
                if field.startswith("-"):
                    direction = -1
                    field = field[1:]
                sort_criteria[field] = direction

                # Ensure we unwind the root foreign key path for this sort field
                root_fk = field.split(".")[0]
                if root_fk not in unwind_fields:
                    unwind_fields.append(root_fk)

        # --- Lookup stages ---
        lookup_stages = self.build_lookup_pipeline(self.model, projection)
        pipeline.extend(lookup_stages)

        # --- Unwind for all required FKs (both from projection and sort)
        for fk in set(unwind_fields):
            pipeline.append(
                {
                    "$unwind": {
                        "path": f"${fk}",
                        "preserveNullAndEmptyArrays": True,
                    }
                }
            )

        # --- Sort stage ---
        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        # --- Match ---
        query = await self.query_builder(params)
        if query:
            pipeline.append({"$match": query})

        # --- Projection ---
        if projection:
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = f"${item}"
                else:
                    field_path, alias = item
                    projection_stage[alias] = f"${field_path}"
        else:
            projection_stage = {
                field: f"${field}"
                for field in self.model.__annotations__.keys()
            }

        if additional_value:
            self.add_extra_value_to_pipeline(additional_value, pipeline)

        if external_pipeline:
            pipeline.extend(external_pipeline)

        if projection_stage:
            pipeline.append({"$project": projection_stage})

        # --- Grouping ---
        if group_by_field:
            group_stage = {
                "_id": f"${group_by_field}",
                "original_id": {"$first": "$_id"},
            }
            push_fields = defaultdict(list)
            first_fields: Dict[str, Any] = {}
            for alias, source in projection_stage.items():
                if alias == "_id":
                    continue
                root = source.lstrip("$").split(".")[0]
                if "." in source:
                    push_fields[root].append((alias, f"${alias}"))
                else:
                    first_fields[alias] = f"${alias}"
            for root, fields in push_fields.items():
                group_stage[root] = {"$addToSet": {k: v for k, v in fields}}
            for alias, source in first_fields.items():
                group_stage[alias] = {"$first": source}
            pipeline.append({"$group": group_stage})

            project_after_group = {
                "_id": "$original_id",
                group_by_field: "$_id",
                **{
                    k: 1
                    for k in group_stage
                    if k not in {"_id", "original_id"}
                },
            }
            pipeline.append({"$project": project_after_group})

        # --- Sort after grouping ---
        if sort_criteria and group_by_field:
            pipeline.append({"$sort": sort_criteria})

        # --- Count before pagination ---
        count_result = await self.aggregate(pipeline)
        filtered_data_count = len(count_result)
        if is_total_count:
            return filtered_data_count, []

        # --- Pagination ---
        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        pipeline.append({"$project": {"_id": 0}})

        # --- Final fetch ---
        # data_result = await self.aggregate(pipeline)
        # results = [self.convert_to_serializable(doc) for doc in data_result]
        # return filtered_data_count, results
        results = await self.aggregate(pipeline)

        return (
            filtered_data_count,
            json.loads(json.dumps(results, default=lambda value: str(value))),
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

    async def query_builder(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        This function interprets filter keys that may include custom operator suffixes
        (e.g., `age__gt`, `name__ico`) and converts them into appropriate MongoDB query syntax.

        Supported operators (configured in `settings.query_param_operators`) include:
            - "eq"  : Equality (default operator if no suffix is found)
            - "in"  : Inclusion in a list of values
            - "be"  : Between two values (range query using $gte and $lte)
            - "lt", "lte", "gt", "gte" : Comparison operators
            - "nq"  : Not equal
            - "sw", "ew", "co", "ico", "ieq" : String pattern matching using regular expressions

        Args:
            filters (Dict[str, Any]): A dictionary where keys may include a field name and optional
                operator suffix (e.g., "price__gt") and values are the corresponding filter values.

        Returns:
            Dict[str, Any]: A dictionary formatted for MongoDB queries, using operators like
                `$eq`, `$in`, `$gte`, `$lte`, `$regex`, etc.

        Raises:
            BadRequest: If an unsupported operator is used, or required values are missing (e.g.,
            'be' operator not given exactly two values).

        Example:
            ?master_id._id__in=[6818763ecb22153e7a47d413,6818765acb22153e7a47d415]
            filters = {
                "price__gt": 100,
                "name__ico": "phone",
                "created__be": ["2021-01-01", "2021-12-31"]
            }
            query = await query_builder(filters)
            # Result:
            # {
            #   "price": {"$gt": 100},
            #   "name": {"$regex": re.compile(".*phone.*", re.IGNORECASE)},
            #   "created": {"$gte": <parsed_date>, "$lte": <parsed_date>}
            # }
        """
        query = {}
        OPERATORS = settings.query_param_operators

        for key, value in filters.items():
            result = self.extract_field_and_operator(key, OPERATORS)
            if not result:
                logger.warning(f"Unsupported filter operator in key: {key}")
                continue

            field, operator = result

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

    # ==========================BEST HANDLING FOR EMBEDDED & fK ==================================
    """
    def build_lookup_pipeline(
        self, model: Any, projections: List[Tuple[str, str]]
    ) -> List[Dict[str, Any]]:
        pipeline = []
        visited_paths = set()

        for field_path, _ in projections:
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

        return pipeline

    """

    # =====================================================================

    # def build_lookup_pipeline(
    #     self, model: Any, projections: List[Union[str, Tuple[str, str]]]
    # ) -> List[Dict[str, Any]]:
    #     pipeline = []
    #     visited_paths = set()

    #     for item in projections:
    #         # Handle both "field" and ("field", "alias")
    #         field_path = item if isinstance(item, str) else item[0]

    #         for full_path, model_field in self._resolve_field_path(
    #             model, field_path
    #         ):
    #             if full_path in visited_paths:
    #                 continue
    #             visited_paths.add(full_path)

    #             if hasattr(model_field, "model"):
    #                 collection = (
    #                     model_field.model.Meta.collection._collection.name
    #                 )
    #                 pipeline.append(
    #                     {
    #                         "$lookup": {
    #                             "from": collection,
    #                             "localField": full_path,
    #                             "foreignField": "_id",
    #                             "as": full_path,
    #                         }
    #                     }
    #                 )
    #                 pipeline.append(
    #                     {
    #                         "$unwind": {
    #                             "path": f"${full_path}",
    #                             "preserveNullAndEmptyArrays": True,
    #                         }
    #                     }
    #                 )

    #             elif self.is_embedded_field(model_field):
    #                 pipeline.append(
    #                     {
    #                         "$unwind": {
    #                             "path": f"${full_path}",
    #                             "preserveNullAndEmptyArrays": True,
    #                         }
    #                     }
    #                 )

    #     return pipeline

    # CHOICE WITH EMBEDDED + FK ==========================
    # def build_lookup_pipeline(
    #     self, model: Any, projections: List[Union[str, Tuple[str, str]]]
    # ) -> List[Dict[str, Any]]:
    #     pipeline = []
    #     visited_paths = set()

    #     used_fields = set(
    #         path if isinstance(path, str) else path[0] for path in projections
    #     )

    #     for field_path in used_fields:
    #         for full_path, model_field in self._resolve_field_path(
    #             model, field_path
    #         ):
    #             if full_path in visited_paths:
    #                 continue
    #             visited_paths.add(full_path)

    #             if hasattr(model_field, "model"):
    #                 collection = (
    #                     model_field.model.Meta.collection._collection.name
    #                 )
    #                 pipeline.append(
    #                     {
    #                         "$lookup": {
    #                             "from": collection,
    #                             "localField": full_path,
    #                             "foreignField": "_id",
    #                             "as": full_path,
    #                         }
    #                     }
    #                 )
    #                 pipeline.append(
    #                     {
    #                         "$unwind": {
    #                             "path": f"${full_path}",
    #                             "preserveNullAndEmptyArrays": True,
    #                         }
    #                     }
    #                 )

    #             elif self.is_embedded_field(model_field):
    #                 pipeline.append(
    #                     {
    #                         "$unwind": {
    #                             "path": f"${full_path}",
    #                             "preserveNullAndEmptyArrays": True,
    #                         }
    #                     }
    #                 )

    #     # === Add Choice Label Fields ===
    #     add_fields = self.collect_choice_label_fields(model, used_fields)
    #     if add_fields:
    #         pipeline.append({"$addFields": add_fields})

    #     return pipeline

    # def is_embedded_field(self, model_field) -> bool:
    #     ann = getattr(model_field, "annotation", None)
    #     if get_origin(ann) is Union:
    #         args = get_args(ann)
    #         non_none_args = [arg for arg in args if arg is not type(None)]
    #         if len(non_none_args) == 1:
    #             ann = non_none_args[0]
    #     if get_origin(ann) is list:
    #         inner = get_args(ann)[0]
    #         return hasattr(inner, "model_fields")
    #     return hasattr(ann, "model_fields")

    # def _resolve_field_path(
    #     self, model: Any, field_path: str
    # ) -> Generator[Tuple[str, Any], None, None]:
    #     """
    #     Yield (full_path, model_field) tuples walking the path from the model.
    #     Handles Unions, lists, embedded docs, ForeignKey relations.
    #     Stops if a field is missing.
    #     """
    #     parts = field_path.split(".")
    #     current_model = model
    #     current_path = ""

    #     for part in parts:
    #         origin = get_origin(current_model)
    #         # Handle Optional / Union
    #         if origin is Union:
    #             args = get_args(current_model)
    #             non_none_args = [arg for arg in args if arg is not type(None)]
    #             if len(non_none_args) == 1:
    #                 current_model = non_none_args[0]
    #                 origin = get_origin(current_model)

    #         # Handle list (e.g. List[EmbeddedDoc])
    #         if origin is list:
    #             current_model = get_args(current_model)[0]

    #         model_fields = getattr(current_model, "model_fields", {})
    #         model_field = model_fields.get(part)
    #         if not model_field:
    #             # Field not found — stop traversal
    #             break

    #         full_path = f"{current_path}.{part}" if current_path else part
    #         yield full_path, model_field

    #         # Update current_model for next iteration
    #         if hasattr(model_field, "model"):
    #             # ForeignKey relation
    #             current_model = model_field.model
    #         elif self.is_embedded_field(model_field):
    #             ann = getattr(model_field, "annotation", None)
    #             ann_origin = get_origin(ann)
    #             if ann_origin is list:
    #                 current_model = get_args(ann)[0]
    #             else:
    #                 current_model = ann
    #         else:
    #             # Leaf field (normal field)
    #             break

    #         current_path = full_path

    # def collect_choice_label_fields(self, model, used_fields: set) -> dict:
    #     add_fields = {}
    #     for field_path in used_fields:
    #         for full_path, model_field in self._resolve_field_path(
    #             model, field_path
    #         ):
    #             # Only check choices on leaf (no deeper model relation)
    #             if not (
    #                 hasattr(model_field, "model")
    #                 or self.is_embedded_field(model_field)
    #             ):
    #                 choices = getattr(model_field, "choices", None)
    #                 if choices:
    #                     label_field = f"{full_path}"
    #                     branches = [
    #                         {
    #                             "case": {"$eq": [f"${full_path}", code]},
    #                             "then": label,
    #                         }
    #                         for code, label in choices
    #                     ]
    #                     add_fields[label_field] = {
    #                         "$switch": {
    #                             "branches": branches,
    #                             "default": f"${full_path}",
    #                         }
    #                     }
    #                 break
    #     return add_fields

    # def _extract_lookups_from_params(
    #     self,
    #     model,
    #     params: dict,
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    # ) -> tuple[list, dict, dict, set]:
    #     if not params and not projection:
    #         return [], {}, {}, set()

    #     lookups = {}
    #     base_params = {}
    #     lookup_params = {}
    #     unwind_paths = set()
    #     OPERATORS = settings.query_param_operators

    #     def add_lookup_chain(field_path: str):
    #         for full_path, model_field in self._resolve_field_path(
    #             model, field_path
    #         ):
    #             if hasattr(model_field, "model"):
    #                 # ForeignKey relation → build lookup if not already present
    #                 if full_path not in lookups:
    #                     collection = (
    #                         model_field.model.meta.collection._collection.name
    #                     )
    #                     lookups[full_path] = {
    #                         "from": collection,
    #                         "localField": full_path,
    #                         "foreignField": "_id",
    #                         "as": full_path,
    #                     }
    #             elif self.is_embedded_field(model_field):
    #                 unwind_paths.add(full_path)
    #             else:
    #                 # Leaf or unknown field: unwind parent if any
    #                 parent_path = ".".join(field_path.split(".")[:-1])
    #                 if parent_path:
    #                     unwind_paths.add(parent_path)
    #                 break

    #     for param_key, param_value in params.items():
    #         result = self.extract_field_and_operator(param_key, OPERATORS)
    #         if not result:
    #             continue
    #         field, _ = result

    #         if "." in field:
    #             add_lookup_chain(field)
    #             lookup_params[param_key] = param_value
    #         else:
    #             base_params[param_key] = param_value

    #     if projection:
    #         for projected_field in projection:
    #             field_path = (
    #                 projected_field[0]
    #                 if isinstance(projected_field, tuple)
    #                 else projected_field
    #             )
    #             if "." in field_path:
    #                 add_lookup_chain(field_path)
    #                 parts = field_path.split(".")
    #                 for i in range(1, len(parts)):
    #                     parent_path = ".".join(parts[:i])
    #                     unwind_paths.add(parent_path)

    #     lookup_stages = []
    #     added_unwinds = set()

    #     for path in sorted(unwind_paths):
    #         if path not in lookups and path not in added_unwinds:
    #             lookup_stages.append(
    #                 {
    #                     "$unwind": {
    #                         "path": f"${path}",
    #                         "preserveNullAndEmptyArrays": True,
    #                     }
    #                 }
    #             )
    #             added_unwinds.add(path)

    #     for full_path, lookup in lookups.items():
    #         lookup_stages.append({"$lookup": lookup})
    #         if full_path not in added_unwinds:
    #             lookup_stages.append(
    #                 {
    #                     "$unwind": {
    #                         "path": f"${full_path}",
    #                         "preserveNullAndEmptyArrays": True,
    #                     }
    #                 }
    #             )
    #             added_unwinds.add(full_path)

    #     used_fields = set()
    #     for key in list(base_params) + list(lookup_params):
    #         field, _ = self.extract_field_and_operator(key, OPERATORS)
    #         used_fields.add(field)
    #     if projection:
    #         for proj in projection:
    #             path = proj[0] if isinstance(proj, tuple) else proj
    #             used_fields.add(path)

    #     add_fields = self.collect_choice_label_fields(model, used_fields)
    #     if add_fields:
    #         lookup_stages.append({"$addFields": add_fields})

    #     return lookup_stages, base_params, lookup_params, unwind_paths

    # def is_embedded_field(self, model_field) -> bool:
    #     ann = getattr(model_field, "annotation", None)
    #     if get_origin(ann) is Union:
    #         args = get_args(ann)
    #         non_none_args = [arg for arg in args if arg is not type(None)]
    #         if len(non_none_args) == 1:
    #             ann = non_none_args[0]
    #     if get_origin(ann) is list:
    #         ann = get_args(ann)[0]
    #     return hasattr(ann, "model_fields")

    '''
    async def search0(
        self,
        params: Dict[str, Any] = {},
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: Optional[Dict[str, int]] = None,
        group_by_field: Optional[str] = None,
        unwind_fields: Optional[List[str]] = [],
        additional_value: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Highly optimized search using smarter lookup generation and unwind handling.
        - Dynamically builds aggregation pipelines with lookups, matches, projections, sorting, and pagination.
        example::
        http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.country_name__sw=I
        http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.continent_id.continent_name__sw=E
        http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.continent_id.continent_name__eq=Asia

        search(
        params=q,
        projection=[
                "name",
                "std",
                ("address.state", "state"),
                ("address.pincode", "pincode"),
                ("address.country_id.country_name", "country_name"),
                ("address.country_id._id", "country_id"),
                (
                    "address.country_id.continent_id.continent_name",
                    "continent_name",
                ),
                ("school_id.name", "school_name"),
                ("school_id.board", "school_board"),
                ("school_id._id", "school_id"),
                ("school_id.university_id.un_name", "university_name"),
                ("school_id.university_id._id", "university_id"),
            ],
        )

        """
        # Initialize params and pipeline components
        params = params or {}
        pipeline = []
        projection_stage = {}
        combined_query = {}
        sort_criteria = {}

        # Extract skip and limit for pagination
        skip_count = int(params.pop("skip", 0))
        limit_count = int(params.pop("pick", 0))

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

        # --- Prepare dynamic lookups based on params and projections ---
        lookup_stages, base_filters, lookup_filters, unwind_paths = (
            self._extract_lookups_from_params(self.model, params, projection)
        )

        # Combine static + dynamic unwind fields
        all_unwinds = set(unwind_paths or []) | set(unwind_fields or [])
        for path in all_unwinds:
            pipeline.append(
                {
                    "$unwind": {
                        "path": f"${path}",  # Keep the '$' here since it's part of the path
                        "preserveNullAndEmptyArrays": True,
                    }
                }
            )

        # Add lookup stages (with $lookup and $unwind) into the pipeline
        pipeline.extend(lookup_stages)

        # --- Match stage: Combine base and lookup filters ---
        if base_filters:
            combined_query.update(self.query_builder(base_filters))
        if lookup_filters:
            combined_query.update(self.query_builder(lookup_filters))

        if combined_query:
            pipeline.append({"$match": combined_query})

        # --- Sort stage ---
        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        # --- Build projection stage ---
        if projection:
            # If custom projection is provided
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = f"${item}"
                elif isinstance(item, tuple):
                    field_path, alias = item
                    # Make sure to only prefix fields with '$' when necessary
                    projection_stage[alias] = f"${field_path}"

        elif not projection:
            all_fields = set()

            # Collect fields used in filters
            for key in list(base_filters.keys()) + list(lookup_filters.keys()):
                field = key.split("__")[0]
                all_fields.add(field)

            # Add unwind paths (embedded fields)
            all_fields.update(unwind_paths)

            # Sort by depth and eliminate parent collisions
            sorted_fields = sorted(all_fields, key=lambda x: x.count("."))
            final_fields = []
            for field in sorted_fields:
                if not any(field.startswith(f + ".") for f in final_fields):
                    final_fields.append(field)

            # Build projection with replaced field names
            projection_stage["_id"] = 1
            for field in final_fields:
                # Replace dots with underscores in field names for safety
                safe_field = field.replace(".", "_")
                projection_stage[safe_field] = f"${field}"

        # Apply base paths to the projection if needed
        if additional_value:
            self.add_extra_value_to_pipeline(additional_value, pipeline)

        # Add projection stage to pipeline
        if projection_stage:
            pipeline.append({"$project": projection_stage})

        # --- Grouping ---
        if group_by_field:
            group_stage = {"_id": f"${group_by_field}"}
            push_fields = defaultdict(list)
            first_fields = {}

            for alias, source in projection_stage.items():
                if alias == group_by_field:
                    continue
                if isinstance(source, str) and "." in source:
                    root = source.split(".")[0].lstrip("$")
                    push_fields[root].append((alias, f"${alias}"))
                else:
                    first_fields[alias] = source

            if push_fields:
                for root, fields in push_fields.items():
                    group_stage[root] = {
                        "$addToSet": {k: v for k, v in fields}
                    }

                # Handle case for group stage with regular fields
                for alias, source in first_fields.items():
                    group_stage[alias] = {"$first": source}

            pipeline.append({"$group": group_stage})

        # --- Pagination stages ---
        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        results = await self.collection.aggregate(
            pipeline, allowDiskUse=True
        ).to_list(length=None)

        return [self.convert_to_serializable(doc) for doc in results]

    def replace_choice_fields_with_display(
        data: Union[Dict, List[Dict]],
        model,
        fields: Optional[List[str]] = None,
    ) -> Union[Dict, List[Dict]]:
        if not data:
            return data

        is_single = isinstance(data, dict)
        documents = [data] if is_single else data

        # Get mapping for all fields with choices
        field_choice_map = {
            field_name: dict(field.field_info.extra.get("choices", []))
            for field_name, field in model.__fields__.items()
            if "choices" in field.field_info.extra
        }

        # Optional filter for specific fields
        if fields:
            field_choice_map = {
                k: v for k, v in field_choice_map.items() if k in fields
            }

        for doc in documents:
            for field_name, choices_dict in field_choice_map.items():
                if field_name in doc and doc[field_name] in choices_dict:
                    doc[field_name] = choices_dict[doc[field_name]]

        return documents[0] if is_single else documents

    # ===========================EMBEDDED ARRAY HARD CODED============WORKING CODE=========
    '''

    '''
    def _extract_lookups_from_params(
            self,
            model,
            params: Dict[str, Any],
            projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        ) -> tuple[list[dict], dict, dict, set[str]]:
            """
            Optimized dynamic deep lookup extractor with proper unwind handling.
            - Dynamically builds MongoDB $lookup and $unwind stages for nested relations.
            - Separates parameters into base fields and lookup fields.
            """

            lookups = {}
            base_params = {}
            lookup_params = {}
            unwind_paths = set()

            def is_embedded_field(model_field) -> bool:
                ann = getattr(model_field, "annotation", None)
                if get_origin(ann) is Union:
                    args = get_args(ann)
                    non_none_args = [arg for arg in args if arg is not type(None)]
                    if len(non_none_args) == 1:
                        ann = non_none_args[0]
                if get_origin(ann) is list:
                    inner = get_args(ann)[0]
                    return hasattr(inner, "model_fields")
                return hasattr(ann, "model_fields")

            def add_lookup_chain(field_path: str):
                parts = field_path.split(".")
                current_model = model
                current_path = ""

                for index, part in enumerate(parts):
                    origin = get_origin(current_model)
                    if origin is Union:
                        args = get_args(current_model)
                        non_none_args = [
                            arg for arg in args if arg is not type(None)
                        ]
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

                    if hasattr(model_field, "model"):  # ForeignKey relation
                        if full_path not in lookups:
                            collection = (
                                model_field.model.meta.collection._collection.name
                            )
                            lookups[full_path] = {
                                "from": collection,
                                "localField": full_path,
                                "foreignField": "_id",
                                "as": full_path,
                                "pipeline": [],
                            }
                        current_model = model_field.model
                        current_path = full_path

                    elif is_embedded_field(model_field):
                        ann = getattr(model_field, "annotation", None)
                        if get_origin(ann) is list:
                            current_model = get_args(ann)[0]
                        else:
                            current_model = ann
                        current_path = full_path
                        unwind_paths.add(full_path)  # <-- add unwind path

                    else:
                        break

            # --- Parse and classify params ---
            for param_key, param_value in params.items():
                base_field_path = param_key.split("__")[0]
                if "." in base_field_path:
                    add_lookup_chain(base_field_path)
                    lookup_params[param_key] = param_value
                else:
                    base_params[param_key] = param_value

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

            # --- Build lookup stages ---
            lookup_stages = []
            for full_path, lookup in lookups.items():
                lookup_stages.append(
                    {
                        "$lookup": {
                            "from": lookup["from"],
                            "localField": lookup["localField"],
                            "foreignField": lookup["foreignField"],
                            "as": lookup["as"],
                        }
                    }
                )
                lookup_stages.append(
                    {
                        "$unwind": {
                            "path": f"${lookup['as']}",
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )

            return lookup_stages, base_params, lookup_params, unwind_paths

        async def search(
            self,
            params: Dict[str, Any] = {},
            projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
            sort: Optional[Dict[str, int]] = None,
            group_by_field: Optional[str] = None,
            unwind_fields: Optional[List[str]] = [],
        ) -> List[Dict[str, Any]]:
            """
            Highly optimized search using smarter lookup generation and unwind handling.
            """
            params = params or {}
            pipeline = []
            projection_stage = {}
            combined_query = {}
            sort_criteria = {}

            skip_count = int(params.pop("skip", 0))
            limit_count = int(params.pop("pick", 0))

            sort_param = params.pop("sort", None)
            if sort_param:
                for field in sort_param.split(","):
                    sort_criteria[
                        field[1:] if field.startswith("-") else field
                    ] = (-1 if field.startswith("-") else 1)

            # Extract lookups and unwind paths
            lookup_stages, base_filters, lookup_filters, unwind_paths = (
                self._extract_lookups_from_params(self.model, params, projection)
            )

            # Apply $unwind for embedded fields before $lookup
            for path in sorted(unwind_paths):
                pipeline.append(
                    {
                        "$unwind": {
                            "path": f"${path}",
                            "preserveNullAndEmptyArrays": True,
                        }
                    }
                )

            # Add $lookup and $unwind (for foreign keys)
            pipeline.extend(lookup_stages)

            # Combine match filters
            if base_filters:
                combined_query.update(self.query_builder(base_filters))
            if lookup_filters:
                combined_query.update(self.query_builder(lookup_filters))

            if combined_query:
                pipeline.append({"$match": combined_query})

            # --- Group for embedded list fields ---
            if group_by_field == "_id":
                group_stage = {
                    "$group": {
                        "_id": "$_id",
                        "name": {"$first": "$name"},
                        "std": {"$first": "$std"},
                        "school_name": {"$first": "$school_id.name"},
                        "addresses": {
                            "$push": {
                                "state": "$address.state",
                                "pincode": "$address.pincode",
                                "country_name": "$address.country_id.country_name",
                                "continent_name": "$address.country_id.continent_id.continent_name",
                            }
                        },
                    }
                }

                replace_root = {
                    "$replaceRoot": {
                        "newRoot": {
                            "$mergeObjects": [
                                {
                                    "_id": "$_id",
                                    "name": "$name",
                                    "std": "$std",
                                    "school_name": "$school_name",
                                },
                                {"addresses": "$addresses"},
                            ]
                        }
                    }
                }

                pipeline.append(group_stage)
                pipeline.append(replace_root)

                # Optional: if you still want to use projection to limit top-level fields
                projection_stage = {
                    "_id": 1,
                    "name": 1,
                    "std": 1,
                    "school_name": 1,
                    "addresses": 1,
                }
                pipeline.append({"$project": projection_stage})
            else:
                # Apply projection if no group_by used
                if projection:
                    for item in projection:
                        if isinstance(item, str):
                            projection_stage[item] = 1
                        elif isinstance(item, tuple):
                            field_path, alias = item
                            projection_stage[alias] = f"${field_path}"
                else:
                    projection_stage = self._build_flattened_projection(self.model)

                if projection_stage:
                    pipeline.append({"$project": projection_stage})

            # Sort
            if sort_criteria:
                pipeline.append({"$sort": sort_criteria})

            # Pagination
            if skip_count:
                pipeline.append({"$skip": skip_count})
            if limit_count:
                pipeline.append({"$limit": limit_count})

            results = await self.collection.aggregate(
                pipeline, allowDiskUse=True
            ).to_list(length=None)
            return [self.convert_to_serializable(doc) for doc in results]
    '''

    # def _build_flattened_projection(self, model, parent_field=""):
    #     """
    #     Recursively builds a flattened MongoDB $project stage for a model.
    #     - Handles nested foreign key relations (references) and embedded documents.
    #     - Flattens nested fields into a simple key -> field mapping.

    #     Args:
    #         model: The Mongoz model to process.
    #         parent_field: The current field path (used for recursion).

    #     Returns:
    #         dict: A projection dictionary suitable for MongoDB aggregation.
    #     """
    #     proj = {}  # Final projection dictionary
    #     fields = model.model_fields  # Get all fields of the model

    #     for model_field, field in fields.items():
    #         # Build full field path (e.g., "school_id.name" or "address.pincode")
    #         full_field = (
    #             f"{parent_field}.{model_field}"
    #             if parent_field
    #             else model_field
    #         )

    #         # --- Handle ForeignKey (Reference field) ---
    #         if hasattr(field, "model"):
    #             # If the field is a foreign key (Reference field)
    #             proj[model_field] = f"${full_field}._id"  # Only _id by default

    #             # Add $lookup for foreign key fields to resolve the reference (fetch details)
    #             proj[model_field] = f"${full_field}"

    #             # Recursively flatten the referenced model fields
    #             proj.update(
    #                 self._build_flattened_projection(field.model, full_field)
    #             )
    #             continue  # Move to next field

    #         # --- Handle EmbeddedDocument (Nested object) ---
    #         if hasattr(field.annotation, "model_fields"):
    #             for (
    #                 sub_field_name,
    #                 sub_field,
    #             ) in field.annotation.model_fields.items():
    #                 # Build full nested path (e.g., "address.country.name")
    #                 nested_full_field = f"{full_field}.{sub_field_name}"

    #                 # Handle EmbeddedDocument or ForeignKey within EmbeddedDocument
    #                 if hasattr(sub_field, "model"):
    #                     # If sub-field is again a ForeignKey (reference)
    #                     proj[sub_field_name] = f"${nested_full_field}._id"
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             sub_field.model, nested_full_field
    #                         )
    #                     )
    #                 elif hasattr(sub_field.annotation, "model_fields"):
    #                     # If sub-field is another level of EmbeddedDocument
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             sub_field.annotation, nested_full_field
    #                         )
    #                     )
    #                 else:
    #                     # Simple field inside embedded document
    #                     proj[sub_field_name] = f"${nested_full_field}"
    #             continue  # Move to next top-level field

    #         # --- Handle List of Embedded Documents ---
    #         if hasattr(field.annotation, "__origin__") and get_origin(
    #             field.annotation
    #         ) in [list, List]:
    #             args = get_args(field.annotation)
    #             if args:
    #                 inner_type = args[0]
    #                 if hasattr(inner_type, "model_fields"):
    #                     # If the list contains an EmbeddedDocument
    #                     proj[model_field] = f"${full_field}"
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             inner_type, full_field
    #                         )
    #                     )
    #                 elif hasattr(inner_type, "model"):
    #                     # If the list contains a ForeignKey (reference)
    #                     proj[model_field] = f"${full_field}"
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             inner_type, full_field
    #                         )
    #                     )
    #             continue  # Move to next field

    #         # --- Handle Simple Fields (Normal fields like string, int) ---
    #         proj[model_field] = f"${full_field}"

    #     return proj

    # def _build_flattened_projection(self, model, parent_field=""):
    #     """
    #     Recursively builds a flattened MongoDB $project stage for a model.
    #     - Handles foreign key relations (references) and embedded documents.
    #     - Flattens nested fields into a simple key -> field mapping.

    #     Args:
    #         model: The Mongoz model to process.
    #         parent_field: The current field path (used for recursion).

    #     Returns:
    #         dict: A projection dictionary suitable for MongoDB aggregation.
    #     """
    #     proj = {}  # Final projection dictionary
    #     fields = model.model_fields  # Get all fields of the model

    #     for model_field, field in fields.items():
    #         # Build full field path (e.g., "school_id.name" or "address.pincode")
    #         full_field = (
    #             f"{parent_field}.{model_field}"
    #             if parent_field
    #             else model_field
    #         )

    #         # --- Handle ForeignKey (Reference field) ---
    #         if hasattr(field, "model"):
    #             # Reference fields: Store the _id directly
    #             proj[model_field] = f"${full_field}._id"

    #             # Recursively flatten the referenced model fields
    #             proj.update(
    #                 self._build_flattened_projection(field.model, full_field)
    #             )
    #             continue  # Move to next field

    #         # --- Handle EmbeddedDocument (Nested object) ---
    #         if hasattr(field.annotation, "model_fields"):
    #             for (
    #                 sub_field_name,
    #                 sub_field,
    #             ) in field.annotation.model_fields.items():
    #                 # Build full nested path (e.g., "address.country.name")
    #                 nested_full_field = f"{full_field}.{sub_field_name}"

    #                 if hasattr(sub_field, "model"):
    #                     # If sub-field is again a ForeignKey inside embedded
    #                     proj[sub_field_name] = f"${nested_full_field}._id"
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             sub_field.model, nested_full_field
    #                         )
    #                     )
    #                 elif hasattr(sub_field.annotation, "model_fields"):
    #                     # If sub-field is another level of EmbeddedDocument
    #                     proj.update(
    #                         self._build_flattened_projection(
    #                             sub_field.annotation, nested_full_field
    #                         )
    #                     )
    #                 else:
    #                     # Simple field inside embedded document
    #                     proj[sub_field_name] = f"${nested_full_field}"
    #             continue  # Move to next top-level field

    #         # --- Handle Simple Fields (Normal fields like string, int) ---
    #         proj[model_field] = f"${full_field}"

    #     return proj

    # =============================

    # def _extract_lookups_from_params(
    #     self,
    #     model,
    #     params: Dict[str, Any],
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    # ) -> tuple[list[dict], dict, dict]:
    #     """
    #     Optimized dynamic deep lookup extractor with proper unwind handling.
    #     - Dynamically builds MongoDB $lookup and $unwind stages for nested relations.
    #     - Separates parameters into base fields and lookup fields.
    #     """

    #     # Stores lookup definitions for nested references
    #     lookups = {}
    #     # Stores params that don't require lookup (base model fields)
    #     base_params = {}
    #     # Stores params that require lookup (related model fields)
    #     lookup_params = {}

    #     def is_embedded_field(model_field) -> bool:
    #         """Checks if the given field is an embedded model (not a foreign relation)."""
    #         return hasattr(model_field.annotation, "model_fields")

    #     def add_lookup_chain(field_path: str):
    #         """
    #         Builds lookup chain for a given dotted path (e.g., 'school_id.university_id.name').
    #         Registers a $lookup for each referenced model encountered.
    #         """
    #         parts = field_path.split(".")
    #         current_model = model
    #         current_path = ""

    #         for index, part in enumerate(parts):
    #             model_field = current_model.model_fields.get(part)
    #             if not model_field:
    #                 # Stop if the field doesn't exist
    #                 break

    #             # Build the full path progressively (e.g., 'school_id', then 'school_id.university_id', etc.)
    #             full_path = f"{current_path}.{part}" if current_path else part

    #             if hasattr(model_field, "model"):
    #                 # If it's a referenced model (foreign key style), create a lookup if not already added
    #                 if full_path not in lookups:
    #                     collection = (
    #                         model_field.model.meta.collection._collection.name
    #                     )
    #                     lookups[full_path] = {
    #                         "from": collection,
    #                         "localField": full_path,
    #                         "foreignField": "_id",
    #                         "as": full_path,
    #                         # Prepare for possible later optimization (e.g., filtering inside lookup)
    #                         "pipeline": [],
    #                     }
    #                 # Move to next model level for deep chains
    #                 current_model = model_field.model
    #                 current_path = full_path

    #             elif is_embedded_field(model_field):
    #                 # If it's an embedded document, drill down without lookup
    #                 current_model = model_field.annotation
    #                 current_path = full_path

    #             else:
    #                 # Scalar field, stop further drilling
    #                 break

    #     # --- Parse and classify params ---
    #     for param_key, param_value in params.items():
    #         # Handle query operators like '__contain', etc.
    #         base_field_path = param_key.split("__")[0]
    #         if "." in base_field_path:
    #             # If param involves a related field (needs lookup)
    #             add_lookup_chain(base_field_path)
    #             lookup_params[param_key] = param_value
    #         else:
    #             # Base field, no lookup needed
    #             base_params[param_key] = param_value

    #     # --- Parse projections for additional lookups ---
    #     if projection:
    #         for projected_field in projection:
    #             field_path = (
    #                 projected_field[0]
    #                 if isinstance(projected_field, tuple)
    #                 else projected_field
    #             )
    #             if "." in field_path:
    #                 # If a projection requires accessing a nested model field
    #                 add_lookup_chain(field_path)

    #     # --- Build lookup and unwind aggregation stages ---
    #     lookup_stages = []
    #     for full_path, lookup in lookups.items():
    #         # Build $lookup stage
    #         lookup_stage = {
    #             "$lookup": {
    #                 "from": lookup["from"],
    #                 "localField": lookup["localField"],
    #                 "foreignField": lookup["foreignField"],
    #                 "as": lookup["as"],
    #             }
    #         }
    #         lookup_stages.append(lookup_stage)

    #         # Build $unwind stage to flatten lookup result arrays
    #         unwind_stage = {
    #             "$unwind": {
    #                 "path": f"${lookup['as']}",
    #                 # Keep documents even if no matching lookup
    #                 "preserveNullAndEmptyArrays": True,
    #             }
    #         }
    #         lookup_stages.append(unwind_stage)

    #     return lookup_stages, base_params, lookup_params

    # async def search(
    #     self,
    #     params: Dict[str, Any] = {},
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort: Optional[Dict[str, int]] = None,
    #     group_by_field: Optional[str] = None,
    #     unwind_fields: Optional[List[str]] = [],
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Highly optimized search using smarter lookup generation and unwind handling.
    #     - Dynamically builds aggregation pipelines with lookups, matches, projections, sorting, and pagination.
    #     example::
    #     http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.country_name__sw=I

    #     http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.continent_id.continent_name__eq=Asia

    #     search(
    #     params=q,
    #     projection=[
    #             "name",
    #             "std",
    #             ("address.state", "state"),
    #             ("address.pincode", "pincode"),
    #             ("address.country_id.country_name", "country_name"),
    #             ("address.country_id._id", "country_id"),
    #             (
    #                 "address.country_id.continent_id.continent_name",
    #                 "continent_name",
    #             ),
    #             ("school_id.name", "school_name"),
    #             ("school_id.board", "school_board"),
    #             ("school_id._id", "school_id"),
    #             ("school_id.university_id.un_name", "university_name"),
    #             ("school_id.university_id._id", "university_id"),
    #         ],
    #     )

    #     """
    #     # Initialize params and pipeline components
    #     params = params or {}
    #     pipeline = []
    #     projection_stage = {}
    #     combined_query = {}
    #     sort_criteria = {}

    #     # Extract skip and limit for pagination
    #     skip_count = int(params.pop("skip", 0))
    #     limit_count = int(params.pop("pick", 0))

    #     # --- Parse sort fields ---
    #     sort_param = params.pop("sort", None)
    #     if sort_param:
    #         for field in sort_param.split(","):
    #             if field.startswith("-"):
    #                 # Descending sort
    #                 sort_criteria[field[1:]] = -1
    #             else:
    #                 # Ascending sort
    #                 sort_criteria[field] = 1

    #     # --- Prepare dynamic lookups based on params and projections ---
    #     lookup_stages, base_filters, lookup_filters = (
    #         self._extract_lookups_from_params(self.model, params, projection)
    #     )

    #     # Add lookup stages (with $lookup and $unwind) into the pipeline
    #     pipeline.extend(lookup_stages)

    #     # --- Match stage: Combine base and lookup filters ---
    #     if base_filters:
    #         combined_query.update(self.query_builder(base_filters))
    #     if lookup_filters:
    #         combined_query.update(self.query_builder(lookup_filters))

    #     if combined_query:
    #         pipeline.append({"$match": combined_query})

    #     # --- Sort stage ---
    #     if sort_criteria:
    #         pipeline.append({"$sort": sort_criteria})

    #     # --- Build projection stage ---
    #     if projection:
    #         # If custom projection is provided
    #         for item in projection:
    #             if isinstance(item, str):
    #                 projection_stage[item] = 1
    #             elif isinstance(item, tuple):
    #                 field_path, alias = item
    #                 projection_stage[alias] = f"${field_path}"
    #     else:
    #         # Default: Deep flatten everything
    #         projection_stage = self._build_flattened_projection(self.model)

    #     # Add projection stage to pipeline
    #     if projection_stage:
    #         pipeline.append({"$project": projection_stage})

    #     # --- Pagination stages ---
    #     if skip_count:
    #         pipeline.append({"$skip": skip_count})
    #     if limit_count:
    #         pipeline.append({"$limit": limit_count})

    #     results = await self.collection.aggregate(
    #         pipeline, allowDiskUse=True
    #     ).to_list(length=None)

    #     return [self.convert_to_serializable(doc) for doc in results]

    #######################################
    #############Code is working for any scenario==========
    #######################################
    # def extract_lookups_from_params(
    #     self,
    #     model,
    #     params: Dict[str, Any],
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    # ) -> tuple[list[dict], dict, dict]:
    #     """
    #     Handles deep nested lookups and embedded fields dynamically.
    #     """
    #     lookups = {}
    #     base_params = {}
    #     lookup_params = {}

    #     def is_embedded_field(model_field) -> bool:
    #         return hasattr(model_field.annotation, "model_fields")

    #     def add_lookup_chain(field_path: str):
    #         """
    #         Recursively prepare lookups for each level of the path.
    #         Example: school_id.university_id.country_id -> three lookups.
    #         """
    #         parts = field_path.split(".")
    #         current_model = model
    #         current_path = ""

    #         for i, part in enumerate(parts):
    #             model_field = current_model.model_fields.get(part)
    #             if not model_field:
    #                 break

    #             full_path = f"{current_path}.{part}" if current_path else part

    #             if hasattr(model_field, "model"):
    #                 if full_path not in lookups:
    #                     collection = (
    #                         model_field.model.meta.collection._collection.name
    #                     )
    #                     lookups[full_path] = {
    #                         "from": collection,
    #                         "localField": full_path,
    #                         "foreignField": "_id",
    #                         "as": full_path,
    #                     }
    #                 current_model = model_field.model
    #                 current_path = full_path

    #             elif is_embedded_field(model_field):
    #                 current_model = model_field.annotation
    #                 current_path = full_path
    #             else:
    #                 break

    #     # --- Parse params ---
    #     for param_key, param_value in params.items():
    #         clean_key = param_key.split("__")[0]  # Remove __ operators
    #         if "." in clean_key:
    #             add_lookup_chain(clean_key)
    #             if "__" in param_key:
    #                 lookup_params[param_key] = param_value
    #             else:
    #                 lookup_params[param_key] = param_value
    #         else:
    #             base_params[param_key] = param_value

    #     # --- Parse projection ---
    #     if projection:
    #         for projected_field in projection:
    #             field_path = (
    #                 projected_field[0]
    #                 if isinstance(projected_field, tuple)
    #                 else projected_field
    #             )
    #             if "." in field_path:
    #                 add_lookup_chain(field_path)

    #     # --- Build lookup stages ---
    #     lookup_stages = []
    #     for local_field, lookup in lookups.items():
    #         lookup_stages.append({"$lookup": lookup})
    #         lookup_stages.append(
    #             {
    #                 "$unwind": {
    #                     "path": f"${lookup['as']}",
    #                     "preserveNullAndEmptyArrays": True,
    #                 }
    #             }
    #         )

    #     return lookup_stages, base_params, lookup_params

    # async def search(
    #     self,
    #     params: Dict[str, Any] = {},
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort_criteria: Optional[Dict[str, int]] = None,
    #     group_by_field: Optional[str] = None,
    #     unwind_fields: Optional[List[str]] = [],
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Sorting:
    #     http://localhost:8000/api/stu/student?name__sw=R&sort=name,school_id.name
    #     """

    #     # --- Extract & parse pagination ---
    #     params = params or {}
    #     pipeline = []
    #     processed_lookups = set()
    #     projection_stage = {}
    #     combined_query = {}
    #     sort_criteria = {}

    #     skip_count = int(params.pop("skip", 0))
    #     limit_count = int(params.pop("pick", 0))

    #     # --- Parse sort query ---
    #     sort_param = params.pop("sort", None)
    #     if sort_param:
    #         for field in sort_param.split(","):
    #             if field.startswith("-"):
    #                 sort_criteria[field[1:]] = -1
    #             else:
    #                 sort_criteria[field] = 1

    #     # --- Prepare filters & lookups ---
    #     lookup_stages, base_filters, lookup_filters = (
    #         self.extract_lookups_from_params(self.model, params, projection)
    #     )

    #     pipeline.extend(lookup_stages)

    #     # --- Apply filters ---
    #     if base_filters:
    #         combined_query.update(self.query_builder(base_filters))
    #     if lookup_filters:
    #         combined_query.update(self.query_builder(lookup_filters))

    #     if combined_query:
    #         pipeline.append({"$match": combined_query})

    #     # --- Sorting (before projection!) ---
    #     if sort_criteria:
    #         pipeline.append({"$sort": sort_criteria})

    #     # --- Projection ---
    #     if projection:
    #         # Use provided projection
    #         for item in projection:
    #             if isinstance(item, str):
    #                 projection_stage[item] = 1
    #             elif isinstance(item, tuple):
    #                 field_path, alias = item
    #                 projection_stage[alias] = f"${field_path}"
    #     else:
    #         # Auto projection based on model fields
    #         lookup_aliases = [
    #             stage["$lookup"]["as"]
    #             for stage in lookup_stages
    #             if "$lookup" in stage
    #         ]

    #         for model_field, field in self.model.model_fields.items():
    #             if hasattr(field, "model"):
    #                 # Foreign key (looked up)
    #                 if model_field in lookup_aliases:
    #                     projection_stage[model_field] = f"${model_field}._id"
    #                 else:
    #                     projection_stage[model_field] = 1

    #             elif hasattr(field.annotation, "model_fields"):
    #                 # embedded field flattening
    #                 projection_stage.update(
    #                     {
    #                         subfield: f"${model_field}.{subfield}"
    #                         for subfield in field.annotation.model_fields
    #                     }
    #                 )

    #             else:
    #                 # Normal field
    #                 projection_stage[model_field] = 1

    #     # Final projection stage
    #     if projection_stage:
    #         pipeline.append({"$project": projection_stage})

    #     # --- Pagination ---
    #     if skip_count:
    #         pipeline.append({"$skip": skip_count})
    #     if limit_count:
    #         pipeline.append({"$limit": limit_count})

    #     # --- Execute ---
    #     results = await self.collection.aggregate(
    #         pipeline, allowDiskUse=True
    #     ).to_list(length=None)
    #     return [self.convert_to_serializable(doc) for doc in results]

    '''This is working code for sinle layer of fk & embedded=================
    def extract_lookups_from_params(
        self,
        model,
        params: Dict[str, Any],
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> tuple[list[dict], dict, dict]:
        """
        Returns:
            lookup_stages: List of `$lookup` and `$unwind` stages
            base_filters: Filters applied directly to root collection
            lookup_filters: Filters applied to foreign collections via `$match`
        """
        lookups = {}
        base_params = {}
        lookup_params = {}

        # Checks if a field is an embedded document
        def is_embedded_field(model_field) -> bool:
            return hasattr(model_field.annotation, "model_fields")

        # Prepares a lookup stage for a foreign key field
        def add_lookup_for_field(local_field: str):
            if local_field in lookups:
                return

            model_field = model.model_fields.get(local_field)
            if model_field and not is_embedded_field(model_field):
                if hasattr(model_field, "model"):
                    collection = (
                        model_field.model.meta.collection._collection.name
                    )
                    if collection:
                        lookups[local_field] = {
                            "from": collection,
                            "localField": local_field,
                            "foreignField": "_id",
                            "as": local_field,
                        }

        # Separate filters into base model filters and foreign key (lookup) filters
        for param_key, param_value in params.items():
            if "." in param_key:
                top_field, nested_path = param_key.split(".", 1)
                model_field = model.model_fields.get(top_field)

                if model_field and is_embedded_field(model_field):
                    base_params[param_key] = param_value
                else:
                    add_lookup_for_field(top_field)
                    if top_field in lookups:
                        if "__" in nested_path:
                            nested_field, op = nested_path.split("__", 1)
                            lookup_params[
                                f"{top_field}.{nested_field}__{op}"
                            ] = param_value
                        else:
                            lookup_params[f"{top_field}.{nested_path}"] = (
                                param_value
                            )
                    else:
                        base_params[param_key] = param_value
            else:
                base_params[param_key] = param_value

        # Extract lookup requirements from projection fields
        if projection:
            for projected_field in projection:
                field_path = (
                    projected_field[0]
                    if isinstance(projected_field, tuple)
                    else projected_field
                )
                if "." in field_path:
                    top_field, _ = field_path.split(".", 1)
                    model_field = model.model_fields.get(top_field)
                    if model_field and not is_embedded_field(model_field):
                        add_lookup_for_field(top_field)

        # Convert lookups to full `$lookup` + `$unwind` stages
        lookup_stages = []
        for local_field, lookup in lookups.items():
            lookup_stages.append({"$lookup": lookup})
            lookup_stages.append(
                {
                    "$unwind": {
                        "path": f"${lookup['as']}",
                        "preserveNullAndEmptyArrays": True,
                    }
                }
            )

        return lookup_stages, base_params, lookup_params

    async def search(
        self,
        params: Dict[str, Any] = {},
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort_criteria: Optional[Dict[str, int]] = None,
        group_by_field: Optional[str] = None,
        unwind_fields: Optional[List[str]] = [],
    ) -> List[Dict[str, Any]]:
        """
        Sorting:
        http://localhost:8000/api/stu/student?name__sw=R&sort=name,school_id.name
        """

        # --- Extract & parse pagination ---
        params = params or {}
        pipeline = []
        processed_lookups = set()
        projection_stage = {}
        combined_query = {}
        sort_criteria = {}

        skip_count = int(params.pop("skip", 0))
        limit_count = int(params.pop("pick", 0))

        # --- Parse sort query ---
        sort_param = params.pop("sort", None)
        if sort_param:
            for field in sort_param.split(","):
                if field.startswith("-"):
                    sort_criteria[field[1:]] = -1
                else:
                    sort_criteria[field] = 1

        # --- Prepare filters & lookups ---
        lookup_stages, base_filters, lookup_filters = (
            self.extract_lookups_from_params(self.model, params, projection)
        )

        pipeline.extend(lookup_stages)

        # --- Apply filters ---
        if base_filters:
            combined_query.update(self.query_builder(base_filters))
        if lookup_filters:
            combined_query.update(self.query_builder(lookup_filters))

        if combined_query:
            pipeline.append({"$match": combined_query})

        # --- Sorting (before projection!) ---
        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        # --- Projection ---
        if projection:
            # Use provided projection
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = 1
                elif isinstance(item, tuple):
                    field_path, alias = item
                    projection_stage[alias] = f"${field_path}"
        else:
            # Auto projection based on model fields
            lookup_aliases = [
                stage["$lookup"]["as"]
                for stage in lookup_stages
                if "$lookup" in stage
            ]

            for model_field, field in self.model.model_fields.items():
                if hasattr(field, "model"):
                    # Foreign key (looked up)
                    if model_field in lookup_aliases:
                        projection_stage[model_field] = f"${model_field}._id"
                    else:
                        projection_stage[model_field] = 1

                elif hasattr(field.annotation, "model_fields"):
                    # embedded field flattening
                    projection_stage.update(
                        {
                            subfield: f"${model_field}.{subfield}"
                            for subfield in field.annotation.model_fields
                        }
                    )

                else:
                    # Normal field
                    projection_stage[model_field] = 1

        # Final projection stage
        if projection_stage:
            pipeline.append({"$project": projection_stage})

        # --- Pagination ---
        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        # --- Execute ---
        results = await self.collection.aggregate(
            pipeline, allowDiskUse=True
        ).to_list(length=None)
        return [self.convert_to_serializable(doc) for doc in results]
    '''
