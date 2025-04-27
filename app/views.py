import os
from typing import Any, Dict, Optional
from bson import ObjectId
from esmerald import Form, Query, Response, get, post, delete, Request, status
from app.commonDao import CommonDAO
from app.llm_service import call_llm
from app.models.app_models import Address, School, Student, UploadedMediaFile
from app.models.stuDao import StudentDAO
from app.utils import generate_response


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


UPLOAD_DIR = "media/uploads/files"


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

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # Save file
    file_path = os.path.join(UPLOAD_DIR, uploaded_file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())

    relative_path = os.path.normpath(file_path).replace(os.sep, "/")

    data = {
        "path": relative_path,
        "type": file_type,
        "name": str(uploaded_file.filename),
    }

    upload_instance = await UploadedMediaFile.objects.create(**data)

    return generate_response(
        request=request,
        data=data,
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

    return generate_response(
        request=request,
        data=stu,
        message="Ok",
    )


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
