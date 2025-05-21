from collections import defaultdict
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
from bson import ObjectId
from esmerald import (
    Form,
    HTTPException,
    Query,
    Response,
    get,
    post,
    delete,
    Request,
    status,
)
from app.commonDao import CommonDAO
from app.llm_service import call_llm
from app.models.app_models import (
    Address,
    School,
    Section,
    Student,
    UploadedMediaFile,
)
from app.models.stuDao import StudentDAO
from app.utils import generate_response, validate_uploaded_files_path


# This is where files are stored in Nginx
# MEDIA_PATH = "/var/lib/playmity/media/"


@get()
@validate_uploaded_files_path()
async def protected_file(
    request: Request,
    rel_file_path: str,
) -> Response:

    MEDIA_PATH = "/data"

    print("rel_file_path =======>", rel_file_path)

    full_path = os.path.join(MEDIA_PATH, rel_file_path)
    print("full_path =======>", full_path)

    # Optional: Validate allowed extensions
    if not rel_file_path.lower().endswith((".jpg", ".png", ".pdf", ".txt")):
        return Response(content="Invalid file type", status_code=400)

    if not os.path.exists(full_path):
        return Response(content="File not found", status_code=404)

    return generate_response(
        request=request,
        extra_headers={"X-Accel-Redirect": f"/privateFile/{rel_file_path}"},
        data={},
        message="Ok",
    )


@post("/upload-file")
async def upload_file(
    logger: Any,
    request: Request,
    data: Any = Form(),
    **kwargs: Any,
) -> Response:
    """Handle file upload."""
    uploaded_file = data.get("path")
    file_type = data.get("type")

    UPLOAD_DIR = os.getenv("UPLOAD_DIR", "data/company_code/uploaded-files")
    ABSOLUTE_UPLOAD_DIR = Path(UPLOAD_DIR)
    ABSOLUTE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Full file system path to save the file
    full_path = os.path.join(ABSOLUTE_UPLOAD_DIR, uploaded_file.filename)
    with open(full_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())

    # Relative path to be stored in DB (used in download)
    relative_path = os.path.normpath(
        os.path.join(
            "data/company_code/uploaded-files", uploaded_file.filename
        )
    ).replace(os.sep, "/")

    data = {
        "path": relative_path,
        "type": file_type,
        "name": str(uploaded_file.filename),
    }

    await UploadedMediaFile.objects.create(**data)

    return generate_response(
        request=request,
        data=data,
        message="Ok",
    )


@post("/generate")
async def llm_response(
    system_prompt: str,
    user_message: str,
    request: Request,
    **kwargs: Any,
) -> Response:
    output = await call_llm(system_prompt, user_message)
    return generate_response(
        request=request,
        data={"response": output},
        message="Ok",
    )


@delete("/{upload_file_id:str}", status_code=status.HTTP_200_OK)
async def delete_file(request: Request, upload_file_id: str) -> Response:
    try:
        instance = await UploadedMediaFile.objects.get(
            id=ObjectId(upload_file_id)
        )
        print("==================", instance)
        # Uncomment this if you want to delete the file
        # await instance.delete()

        return generate_response(
            request=request, data={}, message="File deleted successfully"
        )
    except Exception as e:
        return generate_response(
            request=request, data={}, message=f"Error: {str(e)}"
        )


@post(path="/students", tags=["Students"])
async def create_student(
    request: Request,
    **kwargs: Any,
) -> Response:
    data = await request.json()

    # 1. Extract school and create school document
    school = data.get("school", {})
    school_doc = {"name": school.get("name"), "board": school.get("board")}

    school_obj = await School.objects.create(**school_doc)
    school_id = school_obj.id

    # 2. Prepare embedded address
    address_data = data.get("address", {})
    address = Address(
        village=address_data.get("village"),
        state=address_data.get("state"),
        pincode=address_data.get("pincode"),
    )

    # 3. Create student with FK to school
    student = Student(
        name=data.get("name"),
        std=data.get("std"),
        school_id=school_id,
        address=address,
    )
    await student.save()

    return generate_response(request=request, data={}, message="Ok")


@get("/student")
async def stu_details(
    request: Request,
    q: Optional[Dict[str, Any]] = Query(default=None),
    **kwargs: Any,
) -> Response:
    """Get student details."""
    student_dao = StudentDAO(db="test_database")

    # stu = await student_dao.get_all()

    external_pipeline = [
        # Step 1: Prepare safe join field for generation
        {
            "$addFields": {
                "generation_id_for_join": {
                    "$ifNull": ["$stu_generation_id._id", "$stu_generation_id"]
                }
            }
        },
        # Step 2: Lookup from 'sections' using generation_id
        {
            "$lookup": {
                "from": "sections",
                "let": {"generation_id": "$generation_id_for_join"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$generation_id", "$$generation_id"]
                            }
                        }
                    },
                    {"$project": {"section": 1}},
                ],
                "as": "ref_data",
            }
        },
        # Step 3: Unwind and flatten
        {"$unwind": {"path": "$ref_data", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"section": "$ref_data.section"}},
        {"$project": {"ref_data": 0}},
    ]

    external_pipeline_fk_fk = [
        # Step 1: Add safe join fields
        {
            "$addFields": {
                "school_id_for_join": {
                    "$ifNull": ["$school_id._id", "$school_id"]
                },
                "batch_id_for_join": {
                    "$ifNull": ["$batch_id._id", "$batch_id"]
                },
            }
        },
        # Step 2: Lookup from 'sections' using school_id and batch_id
        {
            "$lookup": {
                "from": "sections",
                "let": {
                    "school_id": "$school_id_for_join",
                    "batch_id": "$batch_id_for_join",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$school_id", "$$school_id"]},
                                    {"$eq": ["$batch_id", "$$batch_id"]},
                                ]
                            }
                        }
                    },
                    {"$project": {"section": 1}},
                ],
                "as": "ref_data",
            }
        },
        # Step 3: Unwind and flatten
        {"$unwind": {"path": "$ref_data", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"section": "$ref_data.section"}},
        {"$project": {"ref_data": 0}},
    ]

    external_pipeline_fk1_simple1 = [
        # Step 1: Add a new field 'school_id_for_join' to ensure proper referencing
        {
            "$addFields": {
                "school_id_for_join": {
                    "$ifNull": ["$school_id._id", "$school_id"]
                }
            }
        },  # Safeguard against null values
        # Step 2: Lookup stage
        {
            "$lookup": {
                "from": "sections",  # Your target collection (e.g., "sections")
                "let": {
                    "school_id": "$school_id_for_join",
                    "std": "$std",
                },  # Use the new field for lookup
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$std", "$$std"]},
                                    {"$eq": ["$school_id", "$$school_id"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {"section": 1}
                    },  # Only return the 'section' field
                ],
                "as": "ref_data",  # Resulting array
            }
        },
        # Step 3: Unwind the 'ref_data' array to flatten the result
        {"$unwind": {"path": "$ref_data", "preserveNullAndEmptyArrays": True}},
        # Step 4: Add 'section' from 'ref_data'
        {"$addFields": {"section": "$ref_data.section"}},
        # Step 5: Remove the 'ref_data' field as it is no longer needed
        {"$project": {"ref_data": 0}},
    ]

    external_pipeline_body_country_name_latest = [
        # (
        #     "school_id.university_id.body.country_id.country_name",
        #     "body_country_name",
        # )
        {
            "$addFields": {
                "school_id_for_join": {
                    "$ifNull": ["$school_id._id", "$school_id"]
                }
            }
        },
        {
            "$lookup": {
                "from": "schools",
                "let": {"school_id": "$school_id_for_join"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$school_id"]}}},
                    {"$project": {"university_id": 1}},
                ],
                "as": "school_data",
            }
        },
        {"$unwind": "$school_data"},
        {
            "$addFields": {
                "university_id_for_join": {
                    "$ifNull": [
                        "$school_data.university_id._id",
                        "$school_data.university_id",
                    ]
                }
            }
        },
        {
            "$lookup": {
                "from": "universities",
                "let": {"university_id": "$university_id_for_join"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$_id", "$$university_id"]}
                        }
                    },
                    {
                        "$project": {
                            "latest_body": {"$arrayElemAt": ["$body", -1]}
                        }
                    },
                ],
                "as": "university_data",
            }
        },
        {"$unwind": "$university_data"},
        {
            "$addFields": {
                "latest_body_country_id_for_join": {
                    "$ifNull": [
                        "$university_data.latest_body.country_id._id",
                        "$university_data.latest_body.country_id",
                    ]
                }
            }
        },
        {
            "$lookup": {
                "from": "countries",
                "let": {"country_id": "$latest_body_country_id_for_join"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$country_id"]}}},
                    {"$project": {"country_name": 1}},
                ],
                "as": "country_data",
            }
        },
        {
            "$unwind": {
                "path": "$country_data",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {"$addFields": {"body_country_name": "$country_data.country_name"}},
    ]

    stu_count, stu_obj = await student_dao.search(
        params=q,
        # is_total_count=True,
        projection=[
            "name",
            "std",
            "roll_no",
            "obtained_pct",
            "is_pass",
            # "section",
            # "body_country_name",
            "mobile_number",
            ("school_id.name", "school_name"),
            ("school_id.board", "school_board"),
            ("school_id.university_id.un_name", "university_name"),
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
        ],
        # additional_value={"school_id.name": "TESTtt"},
        # external_pipeline=external_pipeline_body_country_name_latest,
    )

    return generate_response(
        request=request,
        data={"total_count": stu_count, "result": stu_obj},
        message="Ok",
    )


# add_joined_fields_from_model(
#     pipeline=pipeline,
#     join_model=School,
#     common_fields=[("school_id", "_id"), ("mobile_number", "mobile_number")],
#     joined_fields=["name", "board"]
# )


# unwind_fields=[
#     "address",
#     "address.country_id",
#     "address.country_id.continent_id",
#     "school_id.university_id.body",
#     "school_id.university_id.body.body_name",
# ],
# common_fields=["std", "school_id"],
# join_model=Section,
# joined_fields=["section"],
# group_by_field="_id",

# stu = await student_dao.search(
#         params=q,
#         # unwind_fields=[
#         #     "address",
#         #     "address.country_id",
#         #     "address.country_id.continent_id",
#         #     "school_id.university_id.body",
#         #     "school_id.university_id.body.body_name",
#         # ],
#         # common_fields=["std", "school_id"],
#         # join_model=Section,
#         # joined_fields=["section"],
#         # group_by_field="_id",
#         projection=[
#             "name",
#             #     # "section",
#             #     "std",
#             ("address.village", "village"),
#             #     ("address.state", "state"),
#             #     ("address.pincode", "pincode"),
#             #     ("address.country_id.country_name", "country_name"),
#             #     (
#             #         "address.country_id.continent_id.continent_name",
#             #         "continent_name",
#             #     ),
#             ("school_id.name", "school_name"),
#             #     ("school_id.university_id.un_name", "university_name"),
#             #     ("school_id.university_id.body.body_name", "body_name"),
#             #     (
#             #         "school_id.university_id.body.country_id.country_name",
#             #         "body_country_name",
#             #     ),
#         ],
#         # additional_value={"school_id.name": "ttttttttttttttttt"},
#     )


def transform_search_results(stu: list[dict]) -> list[dict]:
    grouped = defaultdict(
        lambda: {
            "_id": None,
            "name": None,
            "std": None,
            "school_name": None,
            "university_name": None,
            "addresses": [],
        }
    )

    for record in stu:
        student_id = record.get("_id")
        group = grouped[student_id]

        # Set top-level values once
        if group["_id"] is None:
            group["_id"] = student_id
            group["name"] = record.get("name")
            group["std"] = record.get("std")
            group["school_name"] = record.get("school_name")
            group["university_name"] = record.get("university_name")

        # Address-related info
        address = {}
        for key in [
            "village",
            "state",
            "pincode",
            "country_name",
            "continent_name",
        ]:
            if key in record:
                address[key] = record[key]

        # Only append non-empty addresses
        if address and address not in group["addresses"]:
            group["addresses"].append(address)

    return list(grouped.values())
