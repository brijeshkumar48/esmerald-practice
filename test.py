import asyncio
from bson import ObjectId
from app.models.app_models import Student


async def insert_student():
    student_data = {
        "name": "Brijesh",
        "std": "10th",
        "school_id": ObjectId("6453c7e2e7b2f8a9d3c1b009"),
        "address": [
            {
                "village": "Maple Village",
                "state": "Maharashtra",
                "pincode": 400079,
                "country_id": ObjectId("6453c7e2e7b2f8a9d3c1b003"),
            }
        ],
    }

    # Insert the student document into the collection
    new_student = await Student.objects.create(**student_data)
    return new_student


# Run the coroutine
asyncio.run(insert_student())

"""
          ┌────────────────────┐
          │     Postman        │
          │  Upload File API   │
          └────────┬───────────┘
                   │
        ┌──────────▼──────────┐
        │    Esmerald App     │
        │ (/media/uploaded-files) ←──────┐
        └────────┬──────────┬──┘         │ Docker Volume
                 │          │            │
     Save to host│          │ Return     │
    ./media/     │          │ X-Accel    │
   uploaded-files│          │ redirect   │
                 ▼          │            │
         File Path in DB:   │            │
   "media/uploaded-files/.."│            │
                            ▼            │
                        ┌─────────────┐  │
                        │    Nginx    │◄─┘
                        │ /var/lib/...│
                        └─────────────┘
                            │
            Internal redirect serves file

"""


"""
Common DAO
"""

import ast
import importlib
import json
import re
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    get_args,
    get_origin,
)

import logging
from bson.errors import InvalidId
from dateutil import parser
from esmerald import AsyncDAOProtocol
from esmerald.conf import settings
from mongoz import DocumentNotFound, MultipleDocumentsReturned, ObjectId
from pymongo.errors import DuplicateKeyError

from playmity.apps.common.custom_exceptions import BadRequest, ModelNotSupplied

logger = logging.getLogger(__name__)


class CommonDAO(AsyncDAOProtocol):
    """This class represents the common DAO"""

    model = None

    def __init__(
        self,
        *args: list,
        company_db: str = None,
        **kwargs: Dict,
    ) -> None:
        """This is a constructor of this class."""
        super().__init__(*args, **kwargs)
        if not self.model:
            raise ModelNotSupplied("Class atrribute model is not supplied.")
        if company_db:
            self.db = company_db
        else:
            self.db = settings.application_db
        self.collection_name = self.model.Meta.collection.name

    async def get(self, details: Dict) -> model:  # type: ignore
        """This method is used to fetch the document from the DB for the requested details."""
        document = await self.model.objects.using(self.db).get(**details)
        return document

    async def get_all(self, **kwargs: Dict) -> List[model]:  # type: ignore # noqa
        """This method should return the all documents that stored in the
        requested model collection.
        """
        return await self.model.objects.using(self.db).all()

    async def count(self) -> int:
        """This method should return the all documents count that stored in
        the requested model collection.
        """
        return await self.model.objects.using(self.db).count()

    async def filter(
        self,
        skip: int = 0,
        pick: int = 0,
        fields: List[str] = None,
        **kwargs: Dict,
    ) -> List[Any]:
        """
        Filter documents from the collection with optional field projection and pagination.

        Args:
            skip (int): Number of documents to skip (for pagination).
            pick (int): Number of documents to return (limit).
            fields (List[str], optional): A list of fields to project in the result.
                If provided, only these fields will be returned.
                If a single field is passed and `flat=True` is used in the method logic,
                it returns a flat list of values.
            **kwargs (Dict): Filtering conditions as keyword arguments.

        Returns:
            List[Any]: A list of filtered documents or values based on projection.
        """
        query = self.model.objects.using(self.db).filter(**kwargs)

        if skip:
            query = query.skip(skip)
        if pick:
            query = query.limit(pick)

        if fields:
            if len(fields) == 1:
                return await query.values_list(fields, flat=True)
            return await query.values_list(fields)

        return await query.all()

    async def aggregate(self, pipeline: List) -> List[model]:  # type: ignore # noqa
        """This method is use to aggregate the pipeline from DB."""

        result = await self.model.objects.using(
            self.db
        )._collection._async_aggregate(pipeline)
        return list(result)

    async def update(self, obj_id: ObjectId, **kwargs: Dict) -> None:
        """This method is use to update the document for the requested Id."""
        document = await self.get({"id": obj_id})
        if document:
            await document.update(**kwargs)

    async def delete(self, **kwargs: Dict) -> None:
        """This method is use to delete the document/documents."""
        await self.model.objects.using(self.db).filter(**kwargs).delete()

    async def create(self, **kwargs: Dict) -> Any:
        """This method is use to create the document into the collection."""
        try:
            document = await self.model.objects.using(self.db).create(**kwargs)
            return document
        except DuplicateKeyError as error:
            unique_fields = list(error.details["keyValue"].keys())
            unique_details = {field: kwargs[field] for field in unique_fields}
            logger.info(
                f"Document is already created for the {unique_details}"
                f", where the collection is {self.collection_name}"
            )
            raise error

    async def make_count_pipeline(
        self, base_pipeline: List[Dict]
    ) -> List[Dict]:
        count_pipeline = base_pipeline.copy()
        count_pipeline.append({"$count": "total_count"})
        return count_pipeline

    async def search(
        self,
        params: Dict[str, Any] = {},
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: Optional[Dict[str, int]] = None,
        unwind_fields: Optional[List[str]] = [],
        additional_value: Optional[Dict[str, str]] = None,
        external_pipeline: Optional[List[Dict]] = None,
        is_total_count: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Performs an aggregated search on the model's MongoDB collection with options
        for filtering, sorting, projection, joins, grouping, and pagination.

        This method dynamically constructs a MongoDB aggregation pipeline, supporting:
            - Custom filtering using query parameters (including operators like `__gt`, `__ico`)
            - Foreign key joins via $lookup and $unwind
            - Projection and aliasing of fields
            - Sorting and pagination
            - Optional grouping and re-grouping after array unwinds

        Parameters:
        ----------
        params : Dict[str, Any], optional
            Dictionary of query parameters, including:
                - Filter fields with operators (e.g., "price__gt": 100)
                - "skip": number of documents to skip (for pagination)
                - "pick": number of documents to limit (for pagination)
        projection : Optional[List[Union[str, Tuple[str, str]]]], optional
            List of fields to include in the result. Tuples allow renaming fields:
                e.g., [("user.name", "username"), "created_at"]
        sort : Optional[Dict[str, int]], optional
            Dictionary specifying sort order:
                {"field_name": 1} for ascending, -1 for descending.
        group_by : Optional[str], optional
            Field name used to group results. Supports de-duplication after $unwind stages.

        Returns:
        -------
        List[Dict[str, Any]]
            A list of documents matching the aggregation pipeline, serialized for output.

        Raises:
        ------
        BadRequest
            If any filters contain unsupported operators or invalid structures.

        Steps:
        -----
        This method builds and executes an aggregation pipeline using the following steps:
            - Initialize the pipeline as an empty list.
            - Extract and transform filters using supported query operators.
            - Add $lookup stages for referenced foreign key fields.
            - Add a $match stage to apply base and related filters.
            - If needed, add $unwind stages to flatten array fields.
            - Add a $group stage to re-group data and prevent document duplication.
            - If no group_by is provided, unwind the grouped array to restore flat structure.
            - Add a $sort stage for ordering the results.
            - Add $skip and $limit for pagination.
            - Add a $project stage to shape the final output.
            - Execute the pipeline using the model's aggregate method.

        Example
        -------
        : ?sort=-school_id.name&address.country_id.country_name__sw=I
        : ?sort=-school_id.name&address.country_id.continent_id.continent_name__eq=Asia
        : ?created_at__be=[2024-08-13, 2024-08-15]
        : group_by=_id
        : filtered_data_count, filtered_dat = search(
            params=q,
            additional_value={"school_id.name": "St. "},
            is_total_count=True,
            projection=[
                    "name",--> base class field
                    "std",
                    "section",
                    "mobile_number",
                    "school_id",
                    ("address.state", "state"), --> embedded field
                    ("address.pincode", "pincode"),
                    ("address.country_id.country_name", "country_name"),
                    ("address.country_id._id", "country_id"),
                    (
                        "address.country_id.continent_id.continent_name",
                        "continent_name",
                    ),
                    ("school_id.name", "school_name"), --> foreign key
                    ("school_id.board", "school_board"), --> here "school_id.board" resulting in a flat output "school_board"
                    ("school_id._id", "school_id"),
                    ("school_id.university_id.un_name", "university_name"),
                    ("school_id.university_id._id", "university_id"),
                ],
            additional_value={"path":"http://127.0.0.1:8000/"}
            # this 3 field is for custom query addition
            common_fields=["mobile_number", "school_id"],
            join_model=Section,
            joined_fields=["section"],
        )
        """
        # Initialize params and pipeline components
        params = params or {}
        pipeline = []
        projection_stage = {}
        combined_query = {}
        sort_criteria = {}

        skip = int(params.pop("skip", 0))
        pick = int(params.pop("pick", 0))

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

        # --- Prepare dynamic lookups based on params and projections ---
        lookup_stages, base_filters, lookup_filters, unwind_paths = (
            self._extract_lookups_from_params(self.model, params, projection)
        )

        # Add lookup stages (with $lookup and $unwind) into the pipeline
        pipeline.extend(lookup_stages)

        # --- Match stage: Combine base and lookup filters ---
        if base_filters:
            combined_query.update(await self.query_builder(base_filters))
        if lookup_filters:
            combined_query.update(await self.query_builder(lookup_filters))

        if combined_query:
            pipeline.append({"$match": combined_query})

        # --- Sort stage ---
        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        # --- Build projection stage ---
        if projection:
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = f"${item}"
                elif isinstance(item, tuple):
                    field_path, alias = item
                    projection_stage[alias] = f"${field_path}"
        else:
            for field_name in self.model.__annotations__.keys():
                projection_stage[field_name] = f"${field_name}"

        # Group stage
        if group_by_field:
            group_stage = {"_id": f"${group_by_field}"}
            push_fields = defaultdict(list)
            first_fields = {}

            for alias, source in projection_stage.items():
                if alias == "_id":
                    continue

                if isinstance(source, str):
                    root = source.replace("$", "").split(".")[0]

                    # Group embedded fields under their top-level root
                    if "." in source:
                        push_fields[root].append((alias, f"${alias}"))
                    else:
                        first_fields[alias] = f"${alias}"

            for root, fields in push_fields.items():
                group_stage[root] = {"$addToSet": {k: v for k, v in fields}}

            for alias, source in first_fields.items():
                group_stage[alias] = {"$first": source}

            pipeline.append({"$group": group_stage})

        # Can add any additional value/url in any field
        if additional_value:
            self.add_extra_value_to_pipeline(additional_value, pipeline)

        if external_pipeline:
            pipeline.extend(external_pipeline)

        # --- Add projection stage to pipeline --
        if projection_stage:
            projection_stage["id"] = {"$toString": "$_id"}
            projection_stage["_id"] = 0
            pipeline.append({"$project": projection_stage})

        count_result = await self.aggregate(pipeline)
        len_of_data = len(count_result)
        filtered_data_count = len_of_data if len_of_data > 0 else 0

        if is_total_count:
            return (filtered_data_count, [])

        # --- Pagination stages ---
        if skip:
            pipeline.append({"$skip": skip})
        if pick:
            pipeline.append({"$limit": pick})

        results = await self.aggregate(pipeline)
        return (
            filtered_data_count,
            json.loads(json.dumps(results, default=lambda value: str(value))),
        )

    def add_extra_value_to_pipeline(
        self, additional_value: Dict[str, str], pipeline: List[Dict[str, Any]]
    ):
        pipeline.extend(
            [
                {
                    "$addFields": {
                        field_name: {
                            "$concat": [
                                base_path,
                                (
                                    f"${'.'.join(field_parts[:-1])}.{field_parts[-1]}"
                                    if "." in field_name
                                    else f"${field_name}"
                                ),
                            ]
                        }
                    }
                }
                for field_name, base_path in additional_value.items()
                for field_parts in [field_name.split(".")]
            ]
        )

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

        raise BadRequest(
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

    def _extract_lookups_from_params(
        self,
        model,
        params: Dict[str, Any],
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Extracts MongoDB $lookup stages and separates query parameters into base and lookup-related filters.

        This method identifies which query parameters refer to fields that require `$lookup`
        (i.e., foreign key or related collection references). It constructs the necessary
        `$lookup` pipeline stages and separates the parameters accordingly.

        Parameters:
        ----------
        model : MongozDocument
            The Mongoz model class for which the query is being performed.
        params : Dict[str, Any]
            A dictionary of query parameters, potentially including fields that span relationships.
        projection : Optional[List[Union[str, Tuple[str, str]]]], optional
            A list of fields to project in the final output, potentially triggering $lookups.

        Returns:
        -------
        Tuple[
            List[Dict[str, Any]],  # List of $lookup stage definitions
            Dict[str, Any],        # Base query filters (non-related fields or embedded fields)
            Dict[str, Any]         # Related model query filters (requiring lookup joins)
        ]
        """
        # Return early if both params and projection are empty
        if not params and not projection:
            return [], {}, {}, set()

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

                if hasattr(model_field, "to"):  # ForeignKey relation
                    if full_path not in lookups:
                        collection = model_field.to.Meta.collection.name
                        lookups[full_path] = {
                            "from": collection,
                            "localField": full_path,
                            "foreignField": "_id",
                            "as": full_path,
                        }
                    current_model = model_field.to
                    current_path = full_path

                elif is_embedded_field(model_field):
                    ann = getattr(model_field, "annotation", None)
                    if get_origin(ann) is list:
                        current_model = get_args(ann)[0]
                    else:
                        current_model = ann
                    current_path = full_path
                    unwind_paths.add(full_path)

                else:
                    parent_path = ".".join(parts[:index])
                    if parent_path:
                        unwind_paths.add(parent_path)
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
                    parts = field_path.split(".")
                    for i in range(1, len(parts)):
                        parent_path = ".".join(parts[:i])
                        unwind_paths.add(parent_path)

        # --- Init lookup stages safely ---
        lookup_stages = []
        added_unwinds = set()

        # Unwind embedded paths (non-lookup)
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

        # Build lookup and unwind stages
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

        return lookup_stages, base_params, lookup_params, unwind_paths

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
            # Default to "eq"
            field = key
            operator = "eq"

            # Check for operator suffix
            for op in OPERATORS:
                if key.endswith(f"__{op}"):
                    operator = op
                    field = key.rsplit(f"__{op}", 1)[0]
                    break

            # Raise error if operator is unsupported
            if operator not in OPERATORS:
                raise BadRequest(f"Unsupported operator: {operator}")

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

    async def __query_builder(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        **Arguments:**
            - params (Dict[str, Any]): It contains the requested query
            parameters via the API.

        **Raises:**
            - BadRequest: It raises if the incorrect data provided for the
            query operators.

        **Returns:**
            - Dict[str, Any]: Return the dictionary that contains the match
            cases to perform the search operation.
        This method is use to transform the query parameters with the MongoDB operators to perform search accordingly. Followed steps as follows:
            - Initialize the query as {}.
            - Iterate on the params dictionary.
                - Handle dot notation for embedded documents.
                - Split on "__" to supports nested fields in emebedded
                documents.
                - Intialize the field that will store the field name.
                - Set the default operator as eq.
                - Check if an operator is present.
                - According to the operator add the match query into query
                dictionary against the field.
                - Finally return the query.

        """
        query = {}
        OPERATORS = settings.query_param_operators
        for key, value in params.items():
            # Handle dot notation for embedded documents
            # Split on "__" to supports nested fields in emebedded documents
            keys = key.split("__")

            field = keys[0]
            operator = "eq"  # Default to "eq"

            # Check if an operator is present
            for op in OPERATORS.keys():
                if key.endswith(f"__{op}"):  # Handle both __ and dot notation
                    operator = op
                    field = key.replace(f"__{op}", "")
                    break
            if operator == "in" and isinstance(value, list):
                query[field] = {
                    OPERATORS[operator]: [
                        self.__parse_values(field, val) for val in value
                    ]
                }
            elif (
                operator == "sw"
                or operator == "ew"
                or operator == "co"
                or operator == "ico"
                or operator == "ieq"
            ):
                reges_flag = (
                    "" if operator not in ["ico", "ieq"] else re.I
                )  # Case-insensitive for ico and ieq
                if operator == "sw":
                    regex = f"^{value}"  # Starts with
                elif operator == "ew":
                    regex = f"{value}$"  # Ends with
                elif operator == "co":
                    regex = f".*{value}.*"  # Contains
                elif operator == "ieq":
                    regex = f"^{self.__parse_values(field, value)}$"  # Exact match, case-insensitive
                query[field] = {
                    OPERATORS[operator]: (
                        re.compile(regex, reges_flag)
                        if reges_flag
                        else re.compile(regex)
                    )
                }
            elif operator == "be" and isinstance(value, list):
                if len(value) == 2:
                    query[field] = {"$gte": value[0], "$lte": value[1]}
                else:
                    raise BadRequest(
                        f"Invalid value for 'between' operator on field '{field}': {value}"
                    )
            elif operator in ["lt", "lte", "gt", "gte", "nq"]:
                query[field] = {
                    OPERATORS[operator]: self.__parse_values(field, value)
                }
            else:
                query[field] = {
                    OPERATORS[operator]: self.__parse_values(field, value)
                }
        return query


async def search_old(
    self,
    params: Dict[str, Any],
    sort: Dict[str, int] = None,
    group_by: str = None,
    unwind_fields: List[str] = [],
    projection: List[str] = [],
) -> List[Dict[str, Any]]:
    """
     **Type:** Public

    **Arguments:**
        - params (Dict[str, Any]): It contains the query parameters to
          search.
        - sort (Dict[str, int], optional): Contains the sorting
          configutation. Defaults to None.
        - skip (int, optional): Contains the skip element count. Defaults
          to 0.
        - pick (int, optional): Contains the limit element count. Defaults
          to 0.
        - group_by (str, optional): Contains the grouping configutation.
          Defaults to None.
        - projection (List[str], optional): Contains the project fields.
          Defaults to None.

    **Returns:**
        - List[Dict[str, Any]]: _description_

    **Raises:**:
        - BadRequset: If incorrecr params provided.

    **Expamle:**
        - params = {
            "name__sw": "Jhon",
            "age__be": [20, 30],
            "address.city__eq": "New York",
            "age__gt": 25,
            "name__ieq": "john deo",
            "salary__lt"": 5000,
            "is_active__ne": True,
            "address.zipcode__ne": 12345,
            "address.phones__in": [12345,567567],
            "user_id__eq": "67ece4963a011b7a6c189fb5",
            "product_id__nq": "67ece4b33a011b7a6c189fb6"
        }
        - sort = {"age": 1}  # Sort by age ascending
        - projection = [
            "name", "age", "address.city", "address.zipcode",
            "address.phones", "user_id
        ]
    This method is use to search the data based on the query parameters.
    Steps as follows:
        - Initialize the pipeline as [].
        - Transform the query based on query parameters and operators.
        - Add the match query into the pipeline.
        - Unwind the stage for the embedded fields.
        - Group stage to re-group documents after unwind to prevent
        repetition of the same document.
        - Unwind the grouped  array to get the document back.
        - Add the group query in pipeline.
        - Add the sort query in pipeline.
        - Add the pagination configuartion in pipeline.
        - Add the project configuration in pipeline.
        - Fetch the result for the builded pipeline using the aggregate
        method and return the result.
    """
    pipeline = []
    params = dict(params)
    pick = params.pop("pick", 0)
    skip = params.pop("skip", 0)

    # Buid query
    query = await self.__query_builder(params)

    # Match stage (directly matching without unwind)
    pipeline.append({"$match": query})

    # Unwind stage if nedded for array fields (embedded document)
    unwind_fields = (
        set()
    )  # Track fields alreay unwind to avoid repeat unwinding.
    for unwind_field in unwind_fields:
        if (
            unwind_field not in unwind_fields
        ):  # Only unwind once for the same field
            pipeline.append({"$unwind": f"${unwind_field}"})
            unwind_fields.add(unwind_field)

    # Group stage to re-group documents after unwind to prevent repetition of the same document
    # if unwind_fields:
    pipeline.append(
        {
            "$group": {
                "_id": (
                    f"${group_by}" if group_by else 1
                ),  # Group all documents back into one
                "documents": {
                    "$push": "$$ROOT"
                },  # Push original documents to an array
            }
        }
    )
    if not group_by:
        pipeline.append(
            {"$unwind": "$documents"}
        )  # Unwind the grouped  array to get the document back
        pipeline.append(
            {"$replaceRoot": {"newRoot": "$documents"}},
        )
    else:
        projection.append("documents")

    # Sort stage if needed
    if sort:
        pipeline.append({"$sort": sort})

    # Pagination
    if skip:
        pipeline.append({"$skip": skip})
    if pick:
        pipeline.append({"$limit": pick})

    # Projection if needed
    if projection:
        pipeline.append({"$project": {field: 1 for field in projection}})

    result = await self.aggregate(pipeline)
    # Return json parsable data to serve in API response.
    return json.loads(json.dumps(result, default=lambda value: str(value)))


# ===================================================================================================================


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
        # Handle Optional / Union
        if origin is Union:
            args = get_args(current_model)
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1:
                current_model = non_none_args[0]
                origin = get_origin(current_model)

        # Handle list (e.g. List[EmbeddedDoc])
        if origin is list:
            current_model = get_args(current_model)[0]

        model_fields = getattr(current_model, "model_fields", {})
        model_field = model_fields.get(part)
        if not model_field:
            # Field not found — stop traversal
            break

        full_path = f"{current_path}.{part}" if current_path else part
        yield full_path, model_field

        # Update current_model for next iteration
        if hasattr(model_field, "model"):
            # ForeignKey relation
            current_model = model_field.model
        elif self.is_embedded_field(model_field):
            ann = getattr(model_field, "annotation", None)
            ann_origin = get_origin(ann)
            if ann_origin is list:
                current_model = get_args(ann)[0]
            else:
                current_model = ann
        else:
            # Leaf field (normal field)
            # No deeper model to go down
            break

        current_path = full_path


def collect_choice_label_fields(self, model, used_fields: set) -> dict:
    add_fields = {}
    for field_path in used_fields:
        for full_path, model_field in self._resolve_field_path(
            model, field_path
        ):
            # Only check choices on leaf (no deeper model relation)
            if not (
                hasattr(model_field, "model")
                or self.is_embedded_field(model_field)
            ):
                choices = getattr(model_field, "choices", None)
                if choices:
                    label_field = f"{full_path}"
                    branches = [
                        {
                            "case": {"$eq": [f"${full_path}", code]},
                            "then": label,
                        }
                        for code, label in choices
                    ]
                    add_fields[label_field] = {
                        "$switch": {
                            "branches": branches,
                            "default": f"${full_path}",
                        }
                    }
                break
    return add_fields


def _extract_lookups_from_params(
    self,
    model,
    params: dict,
    projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
) -> tuple[list, dict, dict, set]:
    if not params and not projection:
        return [], {}, {}, set()

    lookups = {}
    base_params = {}
    lookup_params = {}
    unwind_paths = set()
    OPERATORS = settings.query_param_operators

    def add_lookup_chain(field_path: str):
        for full_path, model_field in self._resolve_field_path(
            model, field_path
        ):
            if hasattr(model_field, "model"):
                # ForeignKey relation → build lookup if not already present
                if full_path not in lookups:
                    collection = (
                        model_field.model.meta.collection._collection.name
                    )
                    lookups[full_path] = {
                        "from": collection,
                        "localField": full_path,
                        "foreignField": "_id",
                        "as": full_path,
                    }
            elif self.is_embedded_field(model_field):
                unwind_paths.add(full_path)
            else:
                # Leaf or unknown field: unwind parent if any
                parent_path = ".".join(field_path.split(".")[:-1])
                if parent_path:
                    unwind_paths.add(parent_path)
                break

    for param_key, param_value in params.items():
        result = self.extract_field_and_operator(param_key, OPERATORS)
        if not result:
            continue
        field, _ = result

        if "." in field:
            add_lookup_chain(field)
            lookup_params[param_key] = param_value
        else:
            base_params[param_key] = param_value

    if projection:
        for projected_field in projection:
            field_path = (
                projected_field[0]
                if isinstance(projected_field, tuple)
                else projected_field
            )
            if "." in field_path:
                add_lookup_chain(field_path)
                parts = field_path.split(".")
                for i in range(1, len(parts)):
                    parent_path = ".".join(parts[:i])
                    unwind_paths.add(parent_path)

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

    used_fields = set()
    for key in list(base_params) + list(lookup_params):
        field, _ = self.extract_field_and_operator(key, OPERATORS)
        used_fields.add(field)
    if projection:
        for proj in projection:
            path = proj[0] if isinstance(proj, tuple) else proj
            used_fields.add(path)

    add_fields = self.collect_choice_label_fields(model, used_fields)
    if add_fields:
        lookup_stages.append({"$addFields": add_fields})

    return lookup_stages, base_params, lookup_params, unwind_paths


# ===============================================================================================================================================================
# 22/05/2025  ============
# ===============================================================================================================================================================


import ast
import importlib
import json
import re
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
    get_args,
    get_origin,
)

import structlog
from bson.errors import InvalidId
from dateutil import parser
from esmerald import AsyncDAOProtocol
from esmerald.conf import settings
from mongoz import (
    Decimal,
    DocumentNotFound,
    MultipleDocumentsReturned,
    ObjectId,
)
from pymongo.errors import DuplicateKeyError

from playmity.apps.common.custom_exceptions import BadRequest, ModelNotSupplied

logger = structlog.get_logger("common")


class CommonDAO(AsyncDAOProtocol):
    """This class represents the common DAO"""

    model = None

    def __init__(
        self,
        *args: list,
        company_db: str = None,
        **kwargs: Dict,
    ) -> None:
        """This is a constructor of this class."""
        super().__init__(*args, **kwargs)
        if not self.model:
            raise ModelNotSupplied("Class atrribute model is not supplied.")
        if company_db:
            self.db = company_db
        else:
            self.db = settings.application_db
        self.collection_name = self.model.Meta.collection.name

    async def get(self, details: Dict) -> model:  # type: ignore
        """This method is used to fetch the document from the DB for the requested details."""
        document = await self.model.objects.using(self.db).get(**details)
        return document

    async def get_all(self, **kwargs: Dict) -> List[model]:  # type: ignore # noqa
        """This method should return the all documents that stored in the
        requested model collection.
        """
        return await self.model.objects.using(self.db).all()

    async def count(self) -> int:
        """This method should return the all documents count that stored in
        the requested model collection.
        """
        return await self.model.objects.using(self.db).count()

    async def filter(
        self,
        skip: int = 0,
        pick: int = 0,
        fields: List[str] = None,
        **kwargs: Dict,
    ) -> List[Any]:
        """
        Filter documents from the collection with optional field projection and pagination.

        Args:
            skip (int): Number of documents to skip (for pagination).
            pick (int): Number of documents to return (limit).
            fields (List[str], optional): A list of fields to project in the result.
                If provided, only these fields will be returned.
                If a single field is passed and `flat=True` is used in the method logic,
                it returns a flat list of values.
            **kwargs (Dict): Filtering conditions as keyword arguments.

        Returns:
            List[Any]: A list of filtered documents or values based on projection.
        """
        query = self.model.objects.using(self.db).filter(**kwargs)

        if skip:
            query = query.skip(skip)
        if pick:
            query = query.limit(pick)

        if fields:
            if len(fields) == 1:
                return await query.values_list(fields, flat=True)
            return await query.values_list(fields)

        return await query.all()

    async def aggregate(self, pipeline: List) -> List[model]:  # type: ignore # noqa
        """This method is use to aggregate the pipeline from DB."""

        result = await self.model.objects.using(
            self.db
        )._collection._async_aggregate(pipeline)
        return list(result)

    async def update(self, obj_id: ObjectId, **kwargs: Dict) -> None:
        """This method is use to update the document for the requested Id."""
        document = await self.get({"id": obj_id})
        if document:
            await document.update(**kwargs)

    async def delete(self, **kwargs: Dict) -> None:
        """This method is use to delete the document/documents."""
        await self.model.objects.using(self.db).filter(**kwargs).delete()

    async def create(self, **kwargs: Dict) -> Any:
        """This method is use to create the document into the collection."""
        try:
            document = await self.model.objects.using(self.db).create(**kwargs)
            return document
        except DuplicateKeyError as error:
            unique_fields = list(error.details["keyValue"].keys())
            unique_details = {field: kwargs[field] for field in unique_fields}
            logger.info(
                f"Document is already created for the {unique_details}"
                f", where the collection is {self.collection_name}"
            )
            raise error

    async def search(
        self,
        params: Dict[str, Any] = {},
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: Optional[Dict[str, int]] = None,
        unwind_fields: Optional[List[str]] = [],
        additional_value: Optional[Dict[str, str]] = None,
        external_pipeline: Optional[List[Dict]] = None,
        is_total_count: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Performs an aggregated search on the model's MongoDB collection with options
        for filtering, sorting, projection, joins, grouping, and pagination.

        This method dynamically constructs a MongoDB aggregation pipeline, supporting:
            - Custom filtering using query parameters (including operators like `__gt`, `__ico`)
            - Foreign key joins via $lookup and $unwind
            - Projection and aliasing of fields
            - Sorting and pagination
            - Optional grouping and re-grouping after array unwinds

        Parameters:
        ----------
        params : Dict[str, Any], optional
            Dictionary of query parameters, including:
                - Filter fields with operators (e.g., "price__gt": 100)
                - "skip": number of documents to skip (for pagination)
                - "pick": number of documents to limit (for pagination)
        projection : Optional[List[Union[str, Tuple[str, str]]]], optional
            List of fields to include in the result. Tuples allow renaming fields:
                e.g., [("user.name", "username"), "created_at"]
        sort : Optional[Dict[str, int]], optional
            Dictionary specifying sort order:
                {"field_name": 1} for ascending, -1 for descending.
        group_by : Optional[str], optional
            Field name used to group results. Supports de-duplication after $unwind stages.

        Returns:
        -------
        List[Dict[str, Any]]
            A list of documents matching the aggregation pipeline, serialized for output.

        Raises:
        ------
        BadRequest
            If any filters contain unsupported operators or invalid structures.

        Steps:
        -----
        This method builds and executes an aggregation pipeline using the following steps:
            - Initialize the pipeline as an empty list.
            - Extract and transform filters using supported query operators.
            - Add $lookup stages for referenced foreign key fields.
            - Add a $match stage to apply base and related filters.
            - If needed, add $unwind stages to flatten array fields.
            - Add a $group stage to re-group data and prevent document duplication.
            - If no group_by is provided, unwind the grouped array to restore flat structure.
            - Add a $sort stage for ordering the results.
            - Add $skip and $limit for pagination.
            - Add a $project stage to shape the final output.
            - Execute the pipeline using the model's aggregate method.

        Example
        -------
        : ?sort=-school_id.name&address.country_id.country_name__sw=I
        : ?sort=-school_id.name&address.country_id.continent_id.continent_name__eq=Asia
        : ?created_at__be=[2024-08-13, 2024-08-15]
        : group_by=_id
        : filtered_data_count, filtered_dat = search(
            params=q,
            additional_value={"school_id.name": "St. "},
            is_total_count=True,
            projection=[
                    "name",--> base class field
                    "std",
                    "section",
                    "mobile_number",
                    "school_id",
                    ("address.state", "state"), --> embedded field
                    ("address.pincode", "pincode"),
                    ("address.country_id.country_name", "country_name"),
                    ("address.country_id._id", "country_id"),
                    (
                        "address.country_id.continent_id.continent_name",
                        "continent_name",
                    ),
                    ("school_id.name", "school_name"), --> foreign key
                    ("school_id.board", "school_board"), --> here "school_id.board" resulting in a flat output "school_board"
                    ("school_id._id", "school_id"),
                    ("school_id.university_id.un_name", "university_name"),
                    ("school_id.university_id._id", "university_id"),
                ],
            additional_value={"path":"http://127.0.0.1:8000/"}
            # this 3 field is for custom query addition
            common_fields=["mobile_number", "school_id"],
            join_model=Section,
            joined_fields=["section"],
        )
        """
        # Initialize params and pipeline components
        params = params or {}
        pipeline = []
        projection_stage = {}
        combined_query = {}
        sort_criteria = {}

        skip = int(params.pop("skip", 0))
        pick = int(params.pop("pick", 0))

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

        # if external_pipeline:
        #     pipeline.extend(external_pipeline)

        # --- Prepare dynamic lookups based on params and projections ---
        lookup_stages, base_filters, lookup_filters, unwind_paths = (
            self._extract_lookups_from_params(self.model, params, projection)
        )

        # Add lookup stages (with $lookup and $unwind) into the pipeline
        pipeline.extend(lookup_stages)

        # --- Match stage: Combine base and lookup filters ---
        if base_filters:
            combined_query.update(await self.query_builder(base_filters))
        if lookup_filters:
            combined_query.update(await self.query_builder(lookup_filters))

        if combined_query:
            pipeline.append({"$match": combined_query})

        # --- Sort stage ---
        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        # --- Build projection stage ---
        if projection:
            for item in projection:
                if isinstance(item, str):
                    projection_stage[item] = f"${item}"
                else:
                    field_path, alias = item
                    projection_stage[alias] = {
                        "$ifNull": [f"${field_path}", None]
                    }
                # elif isinstance(item, tuple):
                # field_path, alias = item
                # projection_stage[alias] = f"${field_path}"
        else:
            for field_name in self.model.__annotations__.keys():
                projection_stage[field_name] = f"${field_name}"

        # Group stage
        if group_by_field:
            group_stage = {"_id": f"${group_by_field}"}
            push_fields = defaultdict(list)
            first_fields = {}

            for alias, source in projection_stage.items():
                if alias == "_id":
                    continue

                if isinstance(source, str):
                    root = source.replace("$", "").split(".")[0]

                    # Group embedded fields under their top-level root
                    if "." in source:
                        push_fields[root].append((alias, f"${alias}"))
                    else:
                        first_fields[alias] = f"${alias}"

            for root, fields in push_fields.items():
                group_stage[root] = {"$addToSet": {k: v for k, v in fields}}

            for alias, source in first_fields.items():
                group_stage[alias] = {"$first": source}

            pipeline.append({"$group": group_stage})

        # Can add any additional value/url in any field
        if additional_value:
            self.add_extra_value_to_pipeline(additional_value, pipeline)

        if external_pipeline:
            pipeline.extend(external_pipeline)

        pipeline.extend(self.return_choices(self.model, projection))

        # --- Add projection stage to pipeline --
        if projection_stage:
            # projection_stage["id"] = {"$toString": "$_id"}
            projection_stage["_id"] = 0
            pipeline.append({"$project": projection_stage})

        count_result = await self.aggregate(pipeline)
        len_of_data = len(count_result)
        filtered_data_count = len_of_data if len_of_data > 0 else 0

        if is_total_count:
            return (filtered_data_count, [])

        # --- Pagination stages ---
        if skip:
            pipeline.append({"$skip": skip})
        if pick:
            pipeline.append({"$limit": pick})

        results = await self.aggregate(pipeline)

        return (
            filtered_data_count,
            json.loads(json.dumps(results, default=lambda value: str(value))),
        )

    def add_extra_value_to_pipeline(
        self, additional_value: Dict[str, str], pipeline: List[Dict[str, Any]]
    ):
        pipeline.extend(
            [
                {
                    "$addFields": {
                        field_name: {
                            "$concat": [
                                base_path,
                                (
                                    f"${'.'.join(field_parts[:-1])}.{field_parts[-1]}"
                                    if "." in field_name
                                    else f"${field_name}"
                                ),
                            ]
                        }
                    }
                }
                for field_name, base_path in additional_value.items()
                for field_parts in [field_name.split(".")]
            ]
        )

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

        raise BadRequest(
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

        # --- Internal helper function to build lookup chain using _resolve_field_path ---
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
                    parts = field_path.split(".")
                    for i in range(1, len(parts)):
                        unwind_paths.add(".".join(parts[:i]))

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

        return lookup_stages, base_params, lookup_params, unwind_paths

    def _extract_lookups_from_params1(
        self,
        model,
        params: Dict[str, Any],
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Extracts MongoDB $lookup stages and separates query parameters into base and lookup-related filters.

        This method identifies which query parameters refer to fields that require `$lookup`
        (i.e., foreign key or related collection references). It constructs the necessary
        `$lookup` pipeline stages and separates the parameters accordingly.

        Parameters:
        ----------
        model : MongozDocument
            The Mongoz model class for which the query is being performed.
        params : Dict[str, Any]
            A dictionary of query parameters, potentially including fields that span relationships.
        projection : Optional[List[Union[str, Tuple[str, str]]]], optional
            A list of fields to project in the final output, potentially triggering $lookups.

        Returns:
        -------
        Tuple[
            List[Dict[str, Any]],  # List of $lookup stage definitions
            Dict[str, Any],        # Base query filters (non-related fields or embedded fields)
            Dict[str, Any]         # Related model query filters (requiring lookup joins)
        ]
        """
        # Return early if both params and projection are empty
        if not params and not projection:
            return [], {}, {}, set()

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

                if hasattr(model_field, "to"):  # ForeignKey relation
                    if full_path not in lookups:
                        collection = model_field.to.Meta.collection.name
                        lookups[full_path] = {
                            "from": collection,
                            "localField": full_path,
                            "foreignField": "_id",
                            "as": full_path,
                        }
                    current_model = model_field.to
                    current_path = full_path

                elif is_embedded_field(model_field):
                    ann = getattr(model_field, "annotation", None)
                    if get_origin(ann) is list:
                        current_model = get_args(ann)[0]
                    else:
                        current_model = ann
                    current_path = full_path
                    unwind_paths.add(full_path)

                else:
                    parent_path = ".".join(parts[:index])
                    if parent_path:
                        unwind_paths.add(parent_path)
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
                    parts = field_path.split(".")
                    for i in range(1, len(parts)):
                        parent_path = ".".join(parts[:i])
                        unwind_paths.add(parent_path)

        # --- Init lookup stages safely ---
        lookup_stages = []
        added_unwinds = set()

        # Unwind embedded paths (non-lookup)
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

        # Build lookup and unwind stages
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

        return lookup_stages, base_params, lookup_params, unwind_paths

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
            # Default to "eq"
            field = key
            operator = "eq"

            # Check for operator suffix
            for op in OPERATORS:
                if key.endswith(f"__{op}"):
                    operator = op
                    field = key.rsplit(f"__{op}", 1)[0]
                    break

            # Raise error if operator is unsupported
            if operator not in OPERATORS:
                raise BadRequest(f"Unsupported operator: {operator}")

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

    def return_choices(
        self, model: Any, projections: List[Union[str, Tuple[str, str]]]
    ) -> List[Dict[str, Any]]:
        if not projections:
            return []

        pipeline = []

        used_fields = set(
            path if isinstance(path, str) else path[0] for path in projections
        )
        # === Add Choice Label Fields ===
        add_fields = self.collect_choice_label_fields(model, used_fields)
        if add_fields:
            pipeline.append({"$addFields": add_fields})

        return pipeline

    def collect_choice_label_fields(self, model, used_fields: set) -> dict:
        add_fields = {}

        for field_path in used_fields:
            resolved_fields = self._resolve_field_path(model, field_path)

            for full_path, model_field in resolved_fields:
                # Skip if field is relational (has model or is embedded)
                if hasattr(model_field, "to") or self.is_embedded_field(
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

            if hasattr(model_field, "to"):  # Foreign key
                current_model = model_field.to
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

    async def __query_builder(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        **Arguments:**
            - params (Dict[str, Any]): It contains the requested query
            parameters via the API.

        **Raises:**
            - BadRequest: It raises if the incorrect data provided for the
            query operators.

        **Returns:**
            - Dict[str, Any]: Return the dictionary that contains the match
            cases to perform the search operation.
        This method is use to transform the query parameters with the MongoDB operators to perform search accordingly. Followed steps as follows:
            - Initialize the query as {}.
            - Iterate on the params dictionary.
                - Handle dot notation for embedded documents.
                - Split on "__" to supports nested fields in emebedded
                documents.
                - Intialize the field that will store the field name.
                - Set the default operator as eq.
                - Check if an operator is present.
                - According to the operator add the match query into query
                dictionary against the field.
                - Finally return the query.

        """
        query = {}
        OPERATORS = settings.query_param_operators
        for key, value in params.items():
            # Handle dot notation for embedded documents
            # Split on "__" to supports nested fields in emebedded documents
            keys = key.split("__")

            field = keys[0]
            operator = "eq"  # Default to "eq"

            # Check if an operator is present
            for op in OPERATORS.keys():
                if key.endswith(f"__{op}"):  # Handle both __ and dot notation
                    operator = op
                    field = key.replace(f"__{op}", "")
                    break
            if operator == "in" and isinstance(value, list):
                query[field] = {
                    OPERATORS[operator]: [
                        self.__parse_values(field, val) for val in value
                    ]
                }
            elif (
                operator == "sw"
                or operator == "ew"
                or operator == "co"
                or operator == "ico"
                or operator == "ieq"
            ):
                reges_flag = (
                    "" if operator not in ["ico", "ieq"] else re.I
                )  # Case-insensitive for ico and ieq
                if operator == "sw":
                    regex = f"^{value}"  # Starts with
                elif operator == "ew":
                    regex = f"{value}$"  # Ends with
                elif operator == "co":
                    regex = f".*{value}.*"  # Contains
                elif operator == "ieq":
                    regex = f"^{self.__parse_values(field, value)}$"  # Exact match, case-insensitive
                query[field] = {
                    OPERATORS[operator]: (
                        re.compile(regex, reges_flag)
                        if reges_flag
                        else re.compile(regex)
                    )
                }
            elif operator == "be" and isinstance(value, list):
                if len(value) == 2:
                    query[field] = {"$gte": value[0], "$lte": value[1]}
                else:
                    raise BadRequest(
                        f"Invalid value for 'between' operator on field '{field}': {value}"
                    )
            elif operator in ["lt", "lte", "gt", "gte", "nq"]:
                query[field] = {
                    OPERATORS[operator]: self.__parse_values(field, value)
                }
            else:
                query[field] = {
                    OPERATORS[operator]: self.__parse_values(field, value)
                }
        return query

    async def search_old(
        self,
        params: Dict[str, Any],
        sort: Dict[str, int] = None,
        group_by: str = None,
        unwind_fields: List[str] = [],
        projection: List[str] = [],
    ) -> List[Dict[str, Any]]:
        """
        **Type:** Public

        **Arguments:**
            - params (Dict[str, Any]): It contains the query parameters to
            search.
            - sort (Dict[str, int], optional): Contains the sorting
            configutation. Defaults to None.
            - skip (int, optional): Contains the skip element count. Defaults
            to 0.
            - pick (int, optional): Contains the limit element count. Defaults
            to 0.
            - group_by (str, optional): Contains the grouping configutation.
            Defaults to None.
            - projection (List[str], optional): Contains the project fields.
            Defaults to None.

        **Returns:**
            - List[Dict[str, Any]]: _description_

        **Raises:**:
            - BadRequset: If incorrecr params provided.

        **Expamle:**
            - params = {
                "name__sw": "Jhon",
                "age__be": [20, 30],
                "address.city__eq": "New York",
                "age__gt": 25,
                "name__ieq": "john deo",
                "salary__lt"": 5000,
                "is_active__ne": True,
                "address.zipcode__ne": 12345,
                "address.phones__in": [12345,567567],
                "user_id__eq": "67ece4963a011b7a6c189fb5",
                "product_id__nq": "67ece4b33a011b7a6c189fb6"
            }
            - sort = {"age": 1}  # Sort by age ascending
            - projection = [
                "name", "age", "address.city", "address.zipcode",
                "address.phones", "user_id
            ]
        This method is use to search the data based on the query parameters.
        Steps as follows:
            - Initialize the pipeline as [].
            - Transform the query based on query parameters and operators.
            - Add the match query into the pipeline.
            - Unwind the stage for the embedded fields.
            - Group stage to re-group documents after unwind to prevent
            repetition of the same document.
            - Unwind the grouped  array to get the document back.
            - Add the group query in pipeline.
            - Add the sort query in pipeline.
            - Add the pagination configuartion in pipeline.
            - Add the project configuration in pipeline.
            - Fetch the result for the builded pipeline using the aggregate
            method and return the result.
        """
        pipeline = []
        params = dict(params)
        pick = params.pop("pick", 0)
        skip = params.pop("skip", 0)

        # Buid query
        query = await self.__query_builder(params)

        # Match stage (directly matching without unwind)
        pipeline.append({"$match": query})

        # Unwind stage if nedded for array fields (embedded document)
        unwind_fields = (
            set()
        )  # Track fields alreay unwind to avoid repeat unwinding.
        for unwind_field in unwind_fields:
            if (
                unwind_field not in unwind_fields
            ):  # Only unwind once for the same field
                pipeline.append({"$unwind": f"${unwind_field}"})
                unwind_fields.add(unwind_field)

        # Group stage to re-group documents after unwind to prevent repetition of the same document
        # if unwind_fields:
        pipeline.append(
            {
                "$group": {
                    "_id": (
                        f"${group_by}" if group_by else 1
                    ),  # Group all documents back into one
                    "documents": {
                        "$push": "$$ROOT"
                    },  # Push original documents to an array
                }
            }
        )
        if not group_by:
            pipeline.append(
                {"$unwind": "$documents"}
            )  # Unwind the grouped  array to get the document back
            pipeline.append(
                {"$replaceRoot": {"newRoot": "$documents"}},
            )
        else:
            projection.append("documents")

        # Sort stage if needed
        if sort:
            pipeline.append({"$sort": sort})

        # Pagination
        if skip:
            pipeline.append({"$skip": skip})
        if pick:
            pipeline.append({"$limit": pick})

        # Projection if needed
        if projection:
            pipeline.append({"$project": {field: 1 for field in projection}})

        result = await self.aggregate(pipeline)
        # Return json parsable data to serve in API response.
        return json.loads(json.dumps(result, default=lambda value: str(value)))
