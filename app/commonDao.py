from pprint import pformat
from typing import Any, Dict, List, Optional, Union, Tuple, get_type_hints, Annotated, get_args

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
            raise RuntimeError(f"Model {self.model.__name__} does not have a valid 'objects' manager.")
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
    

    def extract_lookup_stages_and_filters(
        self,
        model,
        query_params: Dict[str, Any]
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
        query_params : Dict[str, Any]
            A dictionary of query parameters, potentially including fields that span relationships.

        Returns:
        -------
        Tuple[
            List[Dict[str, Any]],  # List of $lookup stage definitions
            Dict[str, Any],        # Base query filters (non-related fields or embedded fields)
            Dict[str, Any]         # Related model query filters (requiring lookup joins)
        ]
        """
        lookup_stages: Dict[str, Dict[str, Any]] = {}
        base_filters: Dict[str, Any] = {}
        related_model_filters: Dict[str, Any] = {}

        for field_key, field_value in query_params.items():
            if "." in field_key:
                # Handle foreign key or related model field (e.g., "school_id.name")
                base_field, nested_field = field_key.split(".", 1)

                model_field = model.model_fields.get(base_field)
                if model_field and model_field.annotation.__name__ == "EmbeddedDocument":
                    # Embedded fields are part of the same document, treat as base filter
                    base_filters[field_key] = field_value
                    continue

                # Check if the field is a valid reference and fetch the corresponding collection
                referenced_collection = self.get_lookup_collection(model, base_field)
                if referenced_collection:
                    # Create $lookup stage if not already created
                    if base_field not in lookup_stages:
                        lookup_stages[base_field] = {
                            "from": referenced_collection,
                            "localField": base_field,
                            "foreignField": "_id",
                            "as": base_field
                        }
                    # Store filters specific to the joined collection
                    related_model_filters[f"{base_field}.{nested_field}"] = field_value
                else:
                    # If no collection found, treat it as a regular field
                    base_filters[field_key] = field_value
            else:
                # Plain field in the base model
                base_filters[field_key] = field_value

        # Return the $lookup definitions, base filters, and related model filters
        return list(lookup_stages.values()), base_filters, related_model_filters

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
                    query[field] = {"$in": value if isinstance(value, list) else [value]}
                elif op == "contain":
                    query[field] = {"$regex": value, "$options": "i"}
                elif op == "sw":
                    query[field] = {"$regex": f"^{value}", "$options": "i"}
                elif op == "ew":
                    query[field] = {"$regex": f"{value}$", "$options": "i"}
            else:
                query[key] = value
        return query

    def get_lookup_collection(self, model, field_name: str) -> Optional[str]:
        field_info = model.model_fields.get(field_name)
        if not field_info or not field_info.json_schema_extra:
            return None

        meta = field_info.json_schema_extra.get("Meta", {})
        return meta.get("ref")

    def get_lookup_model(self, model, field_name: str):
        field_info = model.model_fields.get(field_name)
        if not field_info or not field_info.json_schema_extra:
            return None

        meta = field_info.json_schema_extra.get("Meta", {})
        return meta.get("ref_model")

    from typing import Dict, Any, Optional, List, Tuple, Union

    async def search(
        self,
        query_params: Dict[str, Any] = {},
        projection_fields: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort_criteria: Optional[Dict[str, int]] = None,
        group_by_field: Optional[str] = None,
        unwind_fields: Optional[List[str]] = [],
    ) -> List[Dict[str, Any]]:
        """
        Performs an aggregated search on the model's collection with optional filtering, sorting, 
        projection, grouping and pagination.

        This method builds a MongoDB aggregation pipeline to search the collection, including 
        handling embedded or foreign key fields through lookups, applying filters, projections, 
        sorting, and pagination based on the provided parameters.

        Parameters:
        ----------
        query_params : Dict[str, Any]
            A dictionary of query parameters that includes filter conditions, skip, and pick values.
        projection_fields : Optional[List[Union[str, Tuple[str, str]]]]
            A list of fields to be included in the result set, with optional aliases for nested fields.
        sort_criteria : Optional[Dict[str, int]]
            A dictionary specifying the sorting order (1 for ascending, -1 for descending).
        group_by_field : Optional[str]
            Field to group results by. Not currently implemented in this function.
        unwind_fields : Optional[List[str]]
            A list of fields to be unwound in the aggregation pipeline (for arrays).

        Returns:
        -------
        List[Dict[str, Any]]
            A list of documents from the collection that match the search criteria, transformed into
            a serializable format.
        """
        query_params = query_params or {}
        skip_count = int(query_params.pop("skip", 0))
        limit_count = int(query_params.pop("pick", 0))

        # Extract lookups and separate query parameters into base and lookup filters
        lookups, base_filters, lookup_filters = self.extract_lookup_stages_and_filters(self.model, query_params)

        # Initialize the aggregation pipeline and other variables
        pipeline = []
        processed_lookups = set()
        projection_stage = {}

        # Helper function to add a lookup stage to the pipeline
        def add_lookup_stage(lookup: Dict[str, Any]):
            if lookup["as"] in processed_lookups:
                return
            # Add $lookup stage for related collection
            pipeline.append({"$lookup": lookup})
            # Add $unwind stage for flattening the lookup results
            pipeline.append({
                "$unwind": {
                    "path": f"${lookup['as']}",
                    "preserveNullAndEmptyArrays": True  # Allow nulls if no match in the lookup collection
                }
            })
            processed_lookups.add(lookup["as"])

        # Apply filters for the base (non-related) fields
        if base_filters:
            # Build query for base filters
            base_query = self.query_builder(base_filters)
            pipeline.append({"$match": base_query})

        # Apply filters for the related models (those requiring lookups)
        if lookup_filters:
            # Build query for lookup filters
            lookup_query = self.query_builder(lookup_filters)
            for lookup in lookups:
                add_lookup_stage(lookup)
            pipeline.append({"$match": lookup_query})

        # Apply projections (fields to return) with optional aliases for nested fields
        if projection_fields:
            for field in projection_fields:
                if isinstance(field, str):
                    projection_stage[field] = 1
                elif isinstance(field, tuple):
                    path, alias = field
                    if "." in path:
                        # For nested fields (e.g., "address.city"), project them with an alias
                        projection_stage[alias] = f"${path}"

        # Default projection: include fields and nested fields for related models
        else:
            for field_name, field in self.model.model_fields.items():
                meta = (field.json_schema_extra or {}).get("Meta", {})
                related_model = meta.get("ref_model")
                # <class 'app.models.app_models.School'>
                related_collection = meta.get("ref")
                # related_collection = 'schools'

                # Handle related models with references (e.g., foreign key)
                if related_model and related_collection:
                    projection_stage.update(dict(map(
                        lambda f: (f"{field_name.replace('_id', '')}_{f}", f"${field_name}.{f}"),
                        filter(lambda f: f != "id", related_model.model_fields)
                    )))

                # Handle embedded models (fields with their own model fields)
                elif hasattr(field.annotation, "model_fields"):
                    projection_stage.update(dict(map(
                        lambda f: (f, f"${field_name}.{f}"),
                        field.annotation.model_fields
                    )))

                else:
                    # Include non-related field in projection
                    projection_stage[field_name] = 1

        if projection_stage:
            pipeline.append({"$project": projection_stage})

        if sort_criteria:
            pipeline.append({"$sort": sort_criteria})

        if skip_count:
            pipeline.append({"$skip": skip_count})
        if limit_count:
            pipeline.append({"$limit": limit_count})

        results = await self.collection.aggregate(pipeline, allowDiskUse=True).to_list(length=None)

        return [self.convert_to_serializable(doc) for doc in results]


    ''' This is working code for all relation filters but not optimized====================

    http://localhost:8000/api/stu/student?name__sw=John

    http://localhost:8000/api/stu/student?school_id.name__sw=Greenwood

    http://localhost:8000/api/stu/student?name__sw=John&school_id.name__sw=Greenwood

    http://localhost:8000/api/stu/student?address.state__sw=Maha

    projection=[
            "name",
            "std",
            ("address.state", "state"),
            ("address.pincode", "pincode"),
            ("school_id.name", "school_name"),
            ("school_id.board", "school_board"),
        ]



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
    
    def extract_lookups_from_params(self, model, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        lookups = {}
        base_params = {}
        lookup_params = {}

        for key, value in params.items():
            if "." in key:
                local_field, rest = key.split(".", 1)
                
                # Skip lookups if this is an embedded field
                model_field = model.model_fields.get(local_field)
                if model_field and model_field.annotation.__name__ == "EmbeddedDocument":
                    base_params[key] = value
                    continue

                collection = self.get_lookup_collection(model, local_field)
                if collection:
                    if local_field not in lookups:
                        lookups[local_field] = {
                            "from": collection,
                            "localField": local_field,
                            "foreignField": "_id",
                            "as": local_field
                        }
                    lookup_params[f"{local_field}.{rest}"] = value
                else:
                    base_params[key] = value

        return list(lookups.values()), base_params, lookup_params


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
                    query[field] = {"$in": value if isinstance(value, list) else [value]}
                elif op == "contain":
                    query[field] = {"$regex": value, "$options": "i"}
                elif op == "sw":  # starts with
                    query[field] = {"$regex": f"^{value}", "$options": "i"}
                elif op == "ew":  # ends with
                    query[field] = {"$regex": f"{value}$", "$options": "i"}
            else:
                query[key] = value
        return query


    def get_lookup_collection(self, model, field_name: str) -> Optional[str]:
        field_info = model.model_fields.get(field_name)
        if not field_info or not field_info.json_schema_extra:
            return None

        meta = field_info.json_schema_extra.get("Meta", {})
        ref = meta.get("ref")

        if ref:
            return f"{ref}"
        return None
    
    def get_lookup_model(self, model, field_name: str):
        field_info = model.model_fields.get(field_name)
        if not field_info or not field_info.json_schema_extra:
            return None

        meta = field_info.json_schema_extra.get("Meta", {})
        return meta.get("ref_model")
    
    async def search(
        self,
        params: Dict[str, Any] = {},
        projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: Optional[Dict[str, int]] = None,
        group_by: Optional[str] = None,
        unwind_fields: Optional[List[str]] = [],
    ) -> List[Dict[str, Any]]:
        params = params or {}
        skip = int(params.pop("skip", 0))
        pick = int(params.pop("pick", 0))

        lookups, base_params, lookup_params = self.extract_lookups_from_params(self.model, params)
        base_query = self.query_builder(base_params)
        lookup_query = self.query_builder(lookup_params)

        pipeline = []
        existing_lookups = set()

        def add_lookup_stage(lookup):
            pipeline.append({"$lookup": lookup})
            pipeline.append({
                "$unwind": {
                    "path": f"${lookup['as']}",
                    "preserveNullAndEmptyArrays": True
                }
            })

        if base_query:
            pipeline.append({"$match": base_query})

        for lookup in lookups:
            if lookup["as"] not in existing_lookups:
                add_lookup_stage(lookup)
                existing_lookups.add(lookup["as"])

        if lookup_query:
            pipeline.append({"$match": lookup_query})

        project_stage = {}

        # Custom projections (user-defined)
        if projection:
            for field in projection:
                if isinstance(field, str):
                    project_stage[field] = 1
                elif isinstance(field, tuple):
                    path, alias = field
                    parts = path.split(".")
                    if len(parts) != 2:
                        continue

                    local_field, foreign_field = parts

                    # Check if it's a foreign key (needs $lookup)
                    if local_field not in existing_lookups:
                        related_collection = self.get_lookup_collection(self.model, local_field)

                        if related_collection:
                            add_lookup_stage({
                                "from": related_collection,
                                "localField": local_field,
                                "foreignField": "_id",
                                "as": local_field,
                            })
                            existing_lookups.add(local_field)

                    # Whether embedded or lookup, just project directly
                    project_stage[alias] = f"${path}"

        # Default projection (if none specified)
        else:
            for field_name, field in self.model.model_fields.items():
                meta = (field.json_schema_extra or {}).get("Meta", {})
                related_model = meta.get("ref_model")
                related_collection = meta.get("ref")

                # For foreign key fields (with $lookup)
                if related_model and related_collection:
                    if field_name not in existing_lookups:
                        add_lookup_stage({
                            "from": related_collection,
                            "localField": field_name,
                            "foreignField": "_id",
                            "as": field_name,
                        })
                        existing_lookups.add(field_name)

                    for related_field in related_model.model_fields:
                        if related_field == "id":
                            continue
                        alias = f"{field_name.replace('_id', '')}_{related_field}"
                        project_stage[alias] = f"${field_name}.{related_field}"

                # For embedded models (flatten fields like 'address.state' => 'state')
                elif hasattr(field.annotation, "model_fields"):
                    for embedded_field in field.annotation.model_fields:
                        alias = embedded_field  # e.g., 'state'
                        project_stage[alias] = f"${field_name}.{embedded_field}"

                else:
                    project_stage[field_name] = 1

        if project_stage:
            pipeline.append({"$project": project_stage})
        if sort:
            pipeline.append({"$sort": sort})
        if skip:
            pipeline.append({"$skip": skip})
        if pick:
            pipeline.append({"$limit": pick})

        results = await self.collection.aggregate(pipeline).to_list(length=None)
        return [self.convert_to_serializable(doc) for doc in results]

    '''
    
    # ======================OK===========FK + projection

    # async def search(
    #     self,
    #     params: Dict[str, Any] = {},
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort: Optional[Dict[str, int]] = None,
    #     group_by: Optional[str] = None,
    #     unwind_fields: Optional[List[str]] = [],
    # ) -> List[Dict[str, Any]]:
    #     params = params or {}
    #     skip = int(params.pop("skip", 0))
    #     pick = int(params.pop("pick", 0))

    #     lookups, base_params, lookup_params = self.extract_lookups_from_params(self.model, params)
    #     base_query = self.query_builder(base_params)
    #     lookup_query = self.query_builder(lookup_params)

    #     pipeline = []
    #     existing_lookups = set()

    #     def add_lookup_stage(lookup):
    #         pipeline.append({"$lookup": lookup})
    #         pipeline.append({
    #             "$unwind": {
    #                 "path": f"${lookup['as']}",
    #                 "preserveNullAndEmptyArrays": True
    #             }
    #         })

    #     if base_query:
    #         pipeline.append({"$match": base_query})

    #     for lookup in lookups:
    #         if lookup["as"] not in existing_lookups:
    #             add_lookup_stage(lookup)
    #             existing_lookups.add(lookup["as"])

    #     if lookup_query:
    #         pipeline.append({"$match": lookup_query})

    #     project_stage = {}

    #     # Custom projections (user-defined)
    #     if projection:
    #         for field in projection:
    #             if isinstance(field, str):
    #                 project_stage[field] = 1
    #             elif isinstance(field, tuple):
    #                 path, alias = field
    #                 parts = path.split(".")
    #                 if len(parts) != 2:
    #                     continue

    #                 local_field, foreign_field = parts
    #                 if local_field not in existing_lookups:
    #                     related_collection = self.get_lookup_collection(self.model, local_field)
    #                     if not related_collection:
    #                         continue

    #                     add_lookup_stage({
    #                         "from": related_collection,
    #                         "localField": local_field,
    #                         "foreignField": "_id",
    #                         "as": local_field,
    #                     })
    #                     existing_lookups.add(local_field)

    #                 project_stage[alias] = f"${local_field}.{foreign_field}"

    #     # Default projection (if none specified)
    #     else:
    #         for field_name, field in self.model.model_fields.items():
    #             meta = (field.json_schema_extra or {}).get("Meta", {})
    #             related_model = meta.get("ref_model")
    #             related_collection = meta.get("ref")

    #             if related_model and related_collection:
    #                 if field_name not in existing_lookups:
    #                     add_lookup_stage({
    #                         "from": related_collection,
    #                         "localField": field_name,
    #                         "foreignField": "_id",
    #                         "as": field_name,
    #                     })
    #                     existing_lookups.add(field_name)

    #                 # Flatten joined fields
    #                 for related_field in related_model.model_fields:
    #                     if related_field == "id":
    #                         continue
    #                     alias = f"{field_name.replace('_id', '')}_{related_field}"
    #                     project_stage[alias] = f"${field_name}.{related_field}"
    #             else:
    #                 project_stage[field_name] = 1

    #     if project_stage:
    #         pipeline.append({"$project": project_stage})
    #     if sort:
    #         pipeline.append({"$sort": sort})
    #     if skip:
    #         pipeline.append({"$skip": skip})
    #     if pick:
    #         pipeline.append({"$limit": pick})

    #     results = await self.collection.aggregate(pipeline).to_list(length=None)
    #     return [self.convert_to_serializable(doc) for doc in results]


    ####################NEW updated with fk projection & q-parms===============


    # async def search(
    #     self,
    #     params: Dict[str, Any] = None,
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort: Optional[Dict[str, int]] = None,
    # ) -> List[Dict[str, Any]]:
        # skip = int(params.pop("skip", 0))
        # pick = int(params.pop("pick", 0))
    #     lookups, base_params, lookup_params = self.extract_lookups_from_params(self.model, params or {})
    #     base_query = self.query_builder(base_params)
    #     lookup_query = self.query_builder(lookup_params)

    #     pipeline = []

    #     def add_lookup_stage(pipeline, lookup):
    #         pipeline.append({"$lookup": lookup})
    #         pipeline.append({
    #             "$unwind": {
    #                 "path": f"${lookup['as']}",
    #                 "preserveNullAndEmptyArrays": True
    #             }
    #         })

    #     if base_query:
    #         pipeline.append({"$match": base_query})

    #     existing_lookups = set()
    #     for lookup in lookups:
    #         if lookup["as"] not in existing_lookups:
    #             add_lookup_stage(pipeline, lookup)
    #             existing_lookups.add(lookup["as"])

    #     if lookup_query:
    #         pipeline.append({"$match": lookup_query})

    #     project_stage = {}
    #     if projection:
    #         for field in projection:
    #             if isinstance(field, str):
    #                 project_stage[field] = 1
    #             elif isinstance(field, tuple):
    #                 path, alias = field
    #                 parts = path.split(".")
    #                 if len(parts) != 2:
    #                     continue

    #                 local_field, foreign_field = parts
    #                 if local_field not in existing_lookups:
    #                     related_collection = self.get_lookup_collection(self.model, local_field)
    #                     if not related_collection:
    #                         continue

    #                     lookup = {
    #                         "from": related_collection,
    #                         "localField": local_field,
    #                         "foreignField": "_id",
    #                         "as": local_field,
    #                     }
    #                     add_lookup_stage(pipeline, lookup)
    #                     existing_lookups.add(local_field)

    #                 project_stage[alias] = f"${local_field}.{foreign_field}"

    #     if project_stage:
    #         pipeline.append({"$project": project_stage})

    #     if sort:
    #         pipeline.append({"$sort": sort})
    #     if skip:
    #         pipeline.append({"$skip": skip})
    #     if pick:
    #         pipeline.append({"$limit": pick})

    #     results = await self.collection.aggregate(pipeline).to_list(length=None)
    #     return [self.convert_to_serializable(doc) for doc in results]

    # #######################################################################


    # def extract_lookups_from_params(self, model, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    #     lookups = {}
    #     rewritten_params = {}

    #     for key, value in params.items():
    #         if "." in key:
    #             local_field, rest = key.split(".", 1)
    #             collection = self.get_lookup_collection(model, local_field)
    #             if collection:
    #                 if local_field not in lookups:
    #                     lookups[local_field] = {
    #                         "from": collection,
    #                         "localField": local_field,
    #                         "foreignField": "_id",
    #                         "as": local_field
    #                     }
    #                 rewritten_params[f"{local_field}.{rest}"] = value
    #             else:
    #                 rewritten_params[key] = value
    #         else:
    #             rewritten_params[key] = value

    #     return list(lookups.values()), rewritten_params
    
    # def extract_lookups_from_params(self, model, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    #     lookups = {}
    #     base_params = {}
    #     lookup_params = {}

    #     for key, value in params.items():
    #         if "." in key:
    #             local_field, rest = key.split(".", 1)
    #             collection = self.get_lookup_collection(model, local_field)
    #             if collection:
    #                 if local_field not in lookups:
    #                     lookups[local_field] = {
    #                         "from": collection,
    #                         "localField": local_field,
    #                         "foreignField": "_id",
    #                         "as": local_field
    #                     }
    #                 lookup_params[f"{local_field}.{rest}"] = value
    #             else:
    #                 base_params[key] = value
    #         else:
    #             base_params[key] = value

    #     return list(lookups.values()), base_params, lookup_params


    # def query_builder(self, filters: Dict[str, Any]) -> Dict[str, Any]:
    #     query = {}
    #     for key, value in filters.items():
    #         if "__" in key:
    #             field, op = key.split("__", 1)
    #             if op == "eq":
    #                 query[field] = value
    #             elif op == "ne":
    #                 query[field] = {"$ne": value}
    #             elif op == "lt":
    #                 query[field] = {"$lt": value}
    #             elif op == "lte":
    #                 query[field] = {"$lte": value}
    #             elif op == "gt":
    #                 query[field] = {"$gt": value}
    #             elif op == "gte":
    #                 query[field] = {"$gte": value}
    #             elif op == "in":
    #                 query[field] = {"$in": value if isinstance(value, list) else [value]}
    #             elif op == "contain":
    #                 query[field] = {"$regex": value, "$options": "i"}
    #             elif op == "sw":  # starts with
    #                 query[field] = {"$regex": f"^{value}", "$options": "i"}
    #             elif op == "ew":  # ends with
    #                 query[field] = {"$regex": f"{value}$", "$options": "i"}
    #         else:
    #             query[key] = value
    #     return query

    # ============================ok for fk projection==============================
    
    # async def search(
    #     self,
    #     params: Dict[str, Any] = None,
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort: Optional[Dict[str, int]] = None,
    # ) -> List[Dict[str, Any]]:
    #     query = self.build_query(params)
    #     pipeline = [{"$match": query}]
    #     project_stage = {}
    #     lookups = {}

    #     if projection:
    #         for field in projection:
    #             if isinstance(field, str):
    #                 project_stage[field] = 1
    #             elif isinstance(field, tuple):
    #                 path, alias = field
    #                 parts = path.split(".")
    #                 if len(parts) != 2:
    #                     continue

    #                 local_field, foreign_field = parts

    #                 related_collection = self.get_lookup_collection(self.model, local_field)
    #                 if not related_collection:
    #                     continue

    #                 if local_field not in lookups:
    #                     lookups[local_field] = {
    #                         "from": related_collection,
    #                         "localField": local_field,
    #                         "foreignField": "_id",
    #                         "as": local_field,
    #                     }

    #                 project_stage[alias] = f"${local_field}.{foreign_field}"

    #     # Add all lookups first
    #     for lookup in lookups.values():
    #         pipeline.append({"$lookup": lookup})
    #         # Flatten single document array
    #         pipeline.append({"$unwind": f"${lookup['as']}"})

    #     if project_stage:
    #         pipeline.append({"$project": project_stage})

    #     if sort:
    #         pipeline.append({"$sort": sort})

    #     results = await self.collection.aggregate(pipeline).to_list(length=None)
    #     results = [self.convert_to_serializable(doc) for doc in results]
    #     return results


    # def convert_to_serializable(self, doc: dict) -> dict:
    #     def convert(value):
    #         if isinstance(value, ObjectId):
    #             return str(value)
    #         elif isinstance(value, dict):
    #             return {k: convert(v) for k, v in value.items()}
    #         elif isinstance(value, list):
    #             return [convert(v) for v in value]
    #         return value

    #     return convert(doc)

    # def get_lookup_collection(self, model, field_name: str) -> Optional[str]:
    #     field_info = model.model_fields.get(field_name)
    #     if not field_info or not field_info.json_schema_extra:
    #         return None

    #     # Extract from nested "Meta" dictionary
    #     meta = field_info.json_schema_extra.get("Meta", {})
    #     ref = meta.get("ref")

    #     if ref:
    #         return f"{ref}"
    #     return None
    
    # ==============================end ok projection========================================
    

    # async def get_all(self) -> List[Dict[str, Any]]:
    #     results = await self.qs().all()
    #     return [self.convert_to_serializable(doc.dict()) for doc in results]
    
    # async def get(self, *args, **filters):
    #     if args:
    #         raise TypeError("CommonDAO.get() only accepts keyword arguments.")
        
    #     query = self.query_builder(filters)
    #     result = await self.qs().filter(**query).first()

    #     if result:
    #         return self.convert_to_serializable(result.dict())
    #     return None

    # async def count(self, **filters) -> int:
    #     query = self.query_builder(filters)
    #     return await self.qs().filter(**query).count()

    # async def filter(self, **filters) -> List[Dict[str, Any]]:
    #     query = self.query_builder(filters)
    #     results = await self.qs().filter(**query).all()
    #     return [doc.dict() for doc in results]

    # async def update(self, filter_params: Dict[str, Any], update_data: Dict[str, Any]) -> int:
    #     query = self.query_builder(filter_params)
    #     return await self.qs().filter(**query).update({"$set": update_data})

    # async def delete(self, **filters) -> int:
    #     query = self.query_builder(filters)
    #     return await self.qs().filter(**query).delete()

    # async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
    #     obj = await self.qs().create(**data)
    #     return obj.dict()
    
    # def query_builder(self, params: Dict[str, Any]) -> Dict[str, Any]:
    #     query = {}
    #     operator_map = {
    #         "eq": "$eq", "ne": "$ne", "nq": "$ne",
    #         "gt": "$gt", "lt": "$lt", "gte": "$gte", "lte": "$lte",
    #         "in": "$in", "nin": "$nin", "between": "$gte",
    #         "ieq": "$regex", "sw": "$regex", "bw": "$regex", "contains": "$regex"
    #     }

    #     for key, value in params.items():
    #         if key in {"id", "_id"}:
    #             try:
    #                 value = ObjectId(value)
    #                 key = "_id"
    #             except Exception:
    #                 pass

    #         if "__" in key:
    #             field, op = key.rsplit("__", 1)
    #             mongo_op = operator_map.get(op)

    #             if op == "between" and isinstance(value, list) and len(value) == 2:
    #                 query[field] = {"$gte": value[0], "$lte": value[1]}
    #             elif op == "ieq":
    #                 query[field] = {mongo_op: f"^{re.escape(value)}$", "$options": "i"}
    #             elif op == "sw":
    #                 query[field] = {mongo_op: f"^{re.escape(value)}", "$options": "i"}
    #             elif op == "bw":
    #                 query[field] = {mongo_op: f"{re.escape(value)}$", "$options": "i"}
    #             elif op == "contains":
    #                 query[field] = {mongo_op: re.escape(value), "$options": "i"}
    #             elif mongo_op:
    #                 query[field] = {mongo_op: value}
    #             else:
    #                 query[field] = value
    #         else:
    #             query[key] = value

    #     return query



# def get_foreign_model(field: str, model: type[BaseDocument]):
    #     annotations = get_type_hints(model)
    #     if field in annotations:
    #         ann = annotations[field]
    #         if getattr(ann, '__origin__', None) is Annotated:
    #             return get_args(ann)[0]
    #     return None


    # def build_query(params: Dict[str, Any]) -> Dict[str, Any]:
    #     query = {}
    #     for k, v in params.items():
    #         if "__" in k:
    #             field, op = k.split("__", 1)
    #             if op == "eq":
    #                 query[field] = v
    #             elif op == "contain":
    #                 query[field] = {"$regex": v, "$options": "i"}
    #             elif op == "in":
    #                 query[field] = {"$in": v}
    #         else:
    #             query[k] = v
    #     return query


    # async def search(
    #     self,
    #     params: Dict[str, Any],
    #     projection: Optional[List[Union[str, Tuple[str, str]]]] = None,
    #     sort: Optional[Dict[str, int]] = None,
    # ) -> List[Dict[str, Any]]:
    #     query = self.build_query(params)
    #     pipeline = [{"$match": query}]

    #     lookups = []
    #     final_projection = {}

    #     if projection:
    #         for item in projection:
    #             if isinstance(item, str):
    #                 final_projection[item] = 1
    #             elif isinstance(item, tuple):
    #                 nested_field, alias = item
    #                 parent_field, child_field = nested_field.split(".", 1)

    #                 fk_model = self.get_foreign_model(parent_field, self.model)
    #                 if fk_model:
    #                     collection_name = getattr(fk_model, '__collection_name__', fk_model.__name__.lower() + "s")
    #                     lookup_stage = {
    #                         "$lookup": {
    #                             "from": collection_name,
    #                             "localField": parent_field,
    #                             "foreignField": "_id",
    #                             "as": f"{parent_field}_info",
    #                         }
    #                     }
    #                     if lookup_stage not in lookups:
    #                         lookups.append(lookup_stage)
    #                         lookups.append({"$unwind": f"${parent_field}_info"})

    #                     final_projection[alias] = f"${parent_field}_info.{child_field}"

    #     pipeline.extend(lookups)

    #     if final_projection:
    #         pipeline.append({"$project": final_projection})

    #     if sort:
    #         pipeline.append({"$sort": sort})

    #     results = await self.collection.aggregate(pipeline).to_list(length=None)
    #     return results

    
    # async def search(
    #     self,
    #     params: Dict[str, Any],
    #     sort: Optional[Dict[str, int]] = None,
    #     group_by: Optional[str] = None,
    #     unwind_fields: Optional[List[str]] = [],
    #     projection: Optional[List[str]] = [],
    # ) -> List[Dict[str, Any]]:
    #     query = self.query_builder(params)
    #     pipeline = [{"$match": query}]

    #     if unwind_fields:
    #         for field in unwind_fields:
    #             if field.endswith("_id"):
    #                 related_collection = field[:-3] + "s"
    #             else:
    #                 related_collection = field + "s"

    #             pipeline.append({
    #                 "$lookup": {
    #                     "from": related_collection,
    #                     "localField": field,
    #                     "foreignField": "_id",
    #                     "as": f"{field}_info"
    #                 }
    #             })

    #             pipeline.append({"$unwind": f"${field}_info"})

    #     if projection:
    #         proj = {field: 1 for field in projection}
    #         pipeline.append({"$project": proj})

    #     if group_by:
    #         pipeline.append({
    #             "$group": {
    #                 "_id": f"${group_by}",
    #                 "items": {"$push": "$$ROOT"}
    #             }
    #         })

    #     if sort:
    #         pipeline.append({"$sort": sort})

    #     results = await self.collection.aggregate(pipeline).to_list(length=None)

    #     return [convert_to_serializable(doc) for doc in results]


    # async def search(
    #     self,
    #     params: Dict[str, Any],
    #     sort: Optional[Dict[str, int]] = None,
    #     group_by: Optional[str] = None,
    #     unwind_fields: Optional[List[str]] = [],
    #     projection: Optional[List[str]] = [],
    # ) -> List[Dict[str, Any]]:
    #     query = self.query_builder(params)
    #     pipeline = [{"$match": query}]

    #     for field in unwind_fields:
    #         pipeline.append({"$unwind": f"${field}"})

    #     if group_by:
    #         pipeline.append({
    #             "$group": {
    #                 "_id": f"${group_by}",
    #                 "items": {"$push": "$$ROOT"}
    #             }
    #         })

    #     if sort:
    #         pipeline.append({"$sort": sort})

    #     if projection:
    #         proj = {field: 1 for field in projection}
    #         pipeline.append({"$project": proj})

    #     # collection = self.get_collection()
    #     results = await self.collection.aggregate(pipeline).to_list(length=None)
    #     return [convert_to_serializable(doc) for doc in results]
