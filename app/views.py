from collections import defaultdict
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
from bson import ObjectId
from esmerald import Form, Query, Response, get, post, delete, Request, status
from app.commonDao import CommonDAO
from app.llm_service import call_llm
from app.models.app_models import Address, School, Student, UploadedMediaFile
from app.models.stuDao import StudentDAO
from app.utils import generate_response, validate_uploaded_files_path


# This is where files are stored in Nginx
# MEDIA_PATH = "/var/lib/playmity/media/"
MEDIA_PATH = "/media"


@get()
@validate_uploaded_files_path()
async def protected_file(request: Request, file_path: str) -> Response:
    print("file_path =======>", file_path)

    full_path = os.path.join(MEDIA_PATH, file_path)
    print("full_path =======>", full_path)

    # Optional: Validate allowed extensions
    if not file_path.lower().endswith((".jpg", ".png", ".pdf", ".txt")):
        return Response(content="Invalid file type", status_code=400)

    if not os.path.exists(full_path):
        return Response(content="File not found", status_code=404)

    return generate_response(
        request=request,
        extra_headers={"X-Accel-Redirect": f"/privateFile/{file_path}"},
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

    # Absolute path where file will be saved inside the container
    # ABSOLUTE_UPLOAD_DIR = "/media/uploaded-files"

    UPLOAD_DIR = os.getenv("UPLOAD_DIR", "media/uploaded-files")
    ABSOLUTE_UPLOAD_DIR = Path(UPLOAD_DIR)
    ABSOLUTE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Full file system path to save the file
    full_path = os.path.join(ABSOLUTE_UPLOAD_DIR, uploaded_file.filename)
    with open(full_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())

    # Relative path to be stored in DB (used in download)
    relative_path = os.path.normpath(
        os.path.join("media/uploaded-files", uploaded_file.filename)
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
    system_prompt: str, user_message: str, request: Request, **kwargs: Any
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

    stu = await student_dao.search(
        params=q,
        # unwind_fields=[
        #     "address",
        #     "address.country_id",
        #     "address.country_id.continent_id",
        #     "school_id.university_id.body",
        #     "school_id.university_id.body.body_name",
        # ],
        group_by_field="name",
        projection=[
            "name",
            "std",
            ("address.village", "village"),
            ("address.state", "state"),
            ("address.pincode", "pincode"),
            ("address.country_id.country_name", "country_name"),
            (
                "address.country_id.continent_id.continent_name",
                "continent_name",
            ),
            ("school_id.name", "school_name"),
            ("school_id.university_id.un_name", "university_name"),
            ("school_id.university_id.body.body_name", "body_name"),
            (
                "school_id.university_id.body.country_id.country_name",
                "body_country_name",
            ),
        ],
        # additional_value={"school_id.name": "ttttttttttttttttt"},
    )

    # formatted_data = transform_search_results(stu)

    return generate_response(
        request=request,
        data=stu,
        message="Ok",
    )


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


# ==============ISSUE==not working proper ==missed country_name in aggregation response===============
"""
pipeline = [
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id",
            "foreignField": "_id",
            "from": "schools",
            "localField": "school_id",
        }
    },
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "as": "school_id.university_id",
            "foreignField": "_id",
            "from": "universities",
            "localField": "school_id.university_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id.university_id.body.country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "school_id.university_id.body.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$project": {
            "body_country_name": "$school_id.university_id.body.country_id.country_name",
            "body_name": "$school_id.university_id.body.body_name",
            "name": "$name",
        }
    },
]


# final pipeline correct
[
    {
        "$lookup": {
            "from": "schools",
            "localField": "school_id",
            "foreignField": "_id",
            "as": "school",
        }
    },
    {"$unwind": {"path": "$school", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "from": "universities",
            "localField": "school.university_id",
            "foreignField": "_id",
            "as": "university",
        }
    },
    {"$unwind": {"path": "$university", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$university.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "university.body.country_id",
            "foreignField": "_id",
            "as": "country",
        }
    },
    {"$unwind": {"path": "$country", "preserveNullAndEmptyArrays": True}},
]
"""


@post("/create_stu")
async def insert_student(request: Request, **kwargs: Any) -> Response:
    student_data = {
        "name": "John Doe",
        "std": "10th",
        "school_id": ObjectId("60c72b2f9b1e8b8b8b8b8b9b"),
    }

    # Insert the student document into the collection
    new_student = await Student.objects.create(**student_data)

    return generate_response(
        request=request,
        data={},
        message="Ok",
    )


[
    {"$unwind": {"path": "$address", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "address.country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "address.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$address.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "address.country_id.continent_id",
            "foreignField": "_id",
            "from": "continents",
            "localField": "address.country_id.continent_id",
        }
    },
    {
        "$unwind": {
            "path": "$address.country_id.continent_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id",
            "foreignField": "_id",
            "from": "schools",
            "localField": "school_id",
        }
    },
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "as": "school_id.university_id",
            "foreignField": "_id",
            "from": "universities",
            "localField": "school_id.university_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id.university_id.body.country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "school_id.university_id.body.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$project": {
            "body_country_name": "$school_id.university_id.body.country_id.country_name",
            "body_name": "$school_id.university_id.body.body_name",
            "continent_name": "$address.country_id.continent_id.continent_name",
            "country_name": "$address.country_id.country_name",
            "name": "$name",
            "pincode": "$address.pincode",
            "school_name": "$school_id.name",
            "state": "$address.state",
            "std": "$std",
            "university_name": "$school_id.university_id.un_name",
            "village": "$address.village",
        }
    },
]
# ===========================================


[
    {"$unwind": {"path": "$address", "preserveNullAndEmptyArrays": True}},
    {
        "$unwind": {
            "path": "$address.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$address.country_id.continent_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id.university_id.body.country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$unwind": {
            "path": "$school_id_university_id.body",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "address_country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "address.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$address_country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "address_country_id_continent_id",
            "foreignField": "_id",
            "from": "continents",
            "localField": "address_country_id.continent_id",
        }
    },
    {
        "$unwind": {
            "path": "$address_country_id_continent_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id",
            "foreignField": "_id",
            "from": "schools",
            "localField": "school_id",
        }
    },
    {"$unwind": {"path": "$school_id", "preserveNullAndEmptyArrays": True}},
    {
        "$lookup": {
            "as": "school_id_university_id",
            "foreignField": "_id",
            "from": "universities",
            "localField": "school_id.university_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id_university_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$lookup": {
            "as": "school_id_university_id_body_country_id",
            "foreignField": "_id",
            "from": "countries",
            "localField": "school_id_university_id.body.country_id",
        }
    },
    {
        "$unwind": {
            "path": "$school_id_university_id_body_country_id",
            "preserveNullAndEmptyArrays": True,
        }
    },
    {
        "$project": {
            "address_country_id": "$address_country_id",
            "address_country_id_continent_id": "$address_country_id_continent_id",
            "body_country_name": "$school_id.university_id.body.country_id.country_name",
            "body_name": "$school_id.university_id.body.body_name",
            "continent_name": "$address.country_id.continent_id.continent_name",
            "country_name": "$address.country_id.country_name",
            "name": "$name",
            "pincode": "$address.pincode",
            "school_id": "$school_id",
            "school_id_university_id": "$school_id_university_id",
            "school_id_university_id_body_country_id": "$school_id_university_id_body_country_id",
            "school_name": "$school_id.name",
            "state": "$address.state",
            "std": "$std",
            "university_name": "$school_id.university_id.un_name",
            "village": "$address.village",
        }
    },
]
