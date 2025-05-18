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
        group_by_field = params.pop("group_by", "_id")

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
        data_result = await self.aggregate(pipeline)
        results = [self.convert_to_serializable(doc) for doc in data_result]
        return filtered_data_count, results

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