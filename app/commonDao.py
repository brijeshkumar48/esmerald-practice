from pprint import pformat
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Tuple,
    get_type_hints,
    Annotated,
    get_args,
)

from pydantic import Field
from app.baseModel import BaseDocument
from bson import ObjectId
from esmerald import AsyncDAOProtocol
from motor.motor_asyncio import AsyncIOMotorClient
import re
from config.settings import settings
from bson import ObjectId
from typing import Any, Dict


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

    def convert_to_serializable(self, doc: dict) -> dict:
        def convert(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert(v) for v in value]
            return value

        return convert(doc)

    def query_builder(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        query = {}
        for key, value in filters.items():
            if "__" in key:
                field, op = key.split("__", 1)
                if op == "eq":
                    query[field] = value
                elif op == "ne":
                    query[field] = {"$ne": value}
                elif op == "lt":
                    query[field] = {"$lt": value}
                elif op == "lte":
                    query[field] = {"$lte": value}
                elif op == "gt":
                    query[field] = {"$gt": value}
                elif op == "gte":
                    query[field] = {"$gte": value}
                elif op == "in":
                    query[field] = {
                        "$in": value if isinstance(value, list) else [value]
                    }
                elif op == "contain":
                    query[field] = {"$regex": value, "$options": "i"}
                elif op == "sw":
                    query[field] = {"$regex": f"^{value}", "$options": "i"}
                elif op == "ew":
                    query[field] = {"$regex": f"{value}$", "$options": "i"}
            else:
                query[key] = value
        return query

    def _build_flattened_projection(self, model, parent_field=""):
        """
        Recursively builds a flattened MongoDB $project stage for a model.
        - Handles foreign key relations (references) and embedded documents.
        - Flattens nested fields into a simple key -> field mapping.

        Args:
            model: The Mongoz model to process.
            parent_field: The current field path (used for recursion).

        Returns:
            dict: A projection dictionary suitable for MongoDB aggregation.
        """
        proj = {}  # Final projection dictionary
        fields = model.model_fields  # Get all fields of the model

        for model_field, field in fields.items():
            # Build full field path (e.g., "school_id.name" or "address.pincode")
            full_field = (
                f"{parent_field}.{model_field}"
                if parent_field
                else model_field
            )

            # --- Handle ForeignKey (Reference field) ---
            if hasattr(field, "model"):
                # Reference fields: Store the _id directly
                proj[model_field] = f"${full_field}._id"

                # Recursively flatten the referenced model fields
                proj.update(
                    self._build_flattened_projection(field.model, full_field)
                )
                continue  # Move to next field

            # --- Handle EmbeddedDocument (Nested object) ---
            if hasattr(field.annotation, "model_fields"):
                for (
                    sub_field_name,
                    sub_field,
                ) in field.annotation.model_fields.items():
                    # Build full nested path (e.g., "address.country.name")
                    nested_full_field = f"{full_field}.{sub_field_name}"

                    if hasattr(sub_field, "model"):
                        # If sub-field is again a ForeignKey inside embedded
                        proj[sub_field_name] = f"${nested_full_field}._id"
                        proj.update(
                            self._build_flattened_projection(
                                sub_field.model, nested_full_field
                            )
                        )
                    elif hasattr(sub_field.annotation, "model_fields"):
                        # If sub-field is another level of EmbeddedDocument
                        proj.update(
                            self._build_flattened_projection(
                                sub_field.annotation, nested_full_field
                            )
                        )
                    else:
                        # Simple field inside embedded document
                        proj[sub_field_name] = f"${nested_full_field}"
                continue  # Move to next top-level field

            # --- Handle Simple Fields (Normal fields like string, int) ---
            proj[model_field] = f"${full_field}"

        return proj

    def _extract_lookups_from_params(
        self,
        model,
        params: Dict[str, Any],
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> tuple[list[dict], dict, dict]:
        """
        Optimized dynamic deep lookup extractor with proper unwind handling.
        - Dynamically builds MongoDB $lookup and $unwind stages for nested relations.
        - Separates parameters into base fields and lookup fields.
        """

        # Stores lookup definitions for nested references
        lookups = {}
        # Stores params that don't require lookup (base model fields)
        base_params = {}
        # Stores params that require lookup (related model fields)
        lookup_params = {}

        def is_embedded_field(model_field) -> bool:
            """Checks if the given field is an embedded model (not a foreign relation)."""
            return hasattr(model_field.annotation, "model_fields")

        def add_lookup_chain(field_path: str):
            """
            Builds lookup chain for a given dotted path (e.g., 'school_id.university_id.name').
            Registers a $lookup for each referenced model encountered.
            """
            parts = field_path.split(".")
            current_model = model
            current_path = ""

            for index, part in enumerate(parts):
                model_field = current_model.model_fields.get(part)
                if not model_field:
                    # Stop if the field doesn't exist
                    break

                # Build the full path progressively (e.g., 'school_id', then 'school_id.university_id', etc.)
                full_path = f"{current_path}.{part}" if current_path else part

                if hasattr(model_field, "model"):
                    # If it's a referenced model (foreign key style), create a lookup if not already added
                    if full_path not in lookups:
                        collection = (
                            model_field.model.meta.collection._collection.name
                        )
                        lookups[full_path] = {
                            "from": collection,
                            "localField": full_path,
                            "foreignField": "_id",
                            "as": full_path,
                            # Prepare for possible later optimization (e.g., filtering inside lookup)
                            "pipeline": [],
                        }
                    # Move to next model level for deep chains
                    current_model = model_field.model
                    current_path = full_path

                elif is_embedded_field(model_field):
                    # If it's an embedded document, drill down without lookup
                    current_model = model_field.annotation
                    current_path = full_path

                else:
                    # Scalar field, stop further drilling
                    break

        # --- Parse and classify params ---
        for param_key, param_value in params.items():
            # Handle query operators like '__contain', etc.
            base_field_path = param_key.split("__")[0]
            if "." in base_field_path:
                # If param involves a related field (needs lookup)
                add_lookup_chain(base_field_path)
                lookup_params[param_key] = param_value
            else:
                # Base field, no lookup needed
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
                    # If a projection requires accessing a nested model field
                    add_lookup_chain(field_path)

        # --- Build lookup and unwind aggregation stages ---
        lookup_stages = []
        for full_path, lookup in lookups.items():
            # Build $lookup stage
            lookup_stage = {
                "$lookup": {
                    "from": lookup["from"],
                    "localField": lookup["localField"],
                    "foreignField": lookup["foreignField"],
                    "as": lookup["as"],
                }
            }
            lookup_stages.append(lookup_stage)

            # Build $unwind stage to flatten lookup result arrays
            unwind_stage = {
                "$unwind": {
                    "path": f"${lookup['as']}",
                    # Keep documents even if no matching lookup
                    "preserveNullAndEmptyArrays": True,
                }
            }
            lookup_stages.append(unwind_stage)

        return lookup_stages, base_params, lookup_params

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
        - Dynamically builds aggregation pipelines with lookups, matches, projections, sorting, and pagination.
        example::
        http://localhost:8000/api/stu/student?sort=-school_id.name&address.country_id.country_name__sw=I

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
        lookup_stages, base_filters, lookup_filters = (
            self._extract_lookups_from_params(self.model, params, projection)
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
                    projection_stage[item] = 1
                elif isinstance(item, tuple):
                    field_path, alias = item
                    projection_stage[alias] = f"${field_path}"
        else:
            # Default: Deep flatten everything
            projection_stage = self._build_flattened_projection(self.model)

        # Add projection stage to pipeline
        if projection_stage:
            pipeline.append({"$project": projection_stage})

        # --- Pagination stages ---
        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        results = await self.collection.aggregate(
            pipeline, allowDiskUse=True
        ).to_list(length=None)

        return [self.convert_to_serializable(doc) for doc in results]

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
