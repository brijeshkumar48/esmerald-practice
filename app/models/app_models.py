from mongoz import String, Document, fields
from typing import Annotated, Any, ClassVar, List, Optional, Type, Union
import mongoz
from pydantic import Field
from app.baseModel import BaseDocument
from db.registry import registry
from bson import ObjectId
from mongoz import Document, EmbeddedDocument
from mongoz.core.db.fields.core import FieldFactory
from mongoz.core.db.fields.base import BaseField


class ForeignKey(FieldFactory):
    _type = ObjectId

    def __new__(
        cls,
        model: Union[Type[Document], Type[EmbeddedDocument]],
        null: bool = False,
        **kwargs: Any,
    ) -> BaseField:
        kwargs = {
            **kwargs,
            "null": null,
            "json_schema_extra": {
                "Meta": {
                    "ref": model.__name__,
                    "ref_model": model,
                }
            },
        }
        return super().__new__(cls, **kwargs)



FileUploadResultTypeChoices = (
    ("SN", "SystemNudge"),
    ("BN", "BusinessNudge"),
    ("B", "Badge"),
)

class UploadedMediaFile(BaseDocument):
    """
    Model representing uploaded media files
    """
    name: str = String(max_length=48)
    path: str = String(max_length=124)
    type: str = String(max_length=2, choices=FileUploadResultTypeChoices)

    class Meta:
        registry = registry
        database = "test_database"

    @staticmethod
    def get_dbtype():
        return "uploaded_media_file"

SchoolBoardChoices = (
    ("ST", "State Board"),
    ("CB", "CBSE"),
    ("IC", "ICSC"),
)

class School(BaseDocument):
    name: str = mongoz.String()
    board: str = mongoz.String(choices = SchoolBoardChoices)

    class Meta:
        registry = registry
        database = "test_database"

    @staticmethod
    def get_dbtype():
        return "schools"
    

class Address(mongoz.EmbeddedDocument):
    village: str = mongoz.String()
    state: str = mongoz.String()
    pincode: str = mongoz.Integer()

    class Meta:
        registry = registry
        database = "test_database"

    @staticmethod
    def get_dbtype():
        return "address"


class Student(BaseDocument):
    name: str = mongoz.String()
    std: str = mongoz.String()
    school_id: ObjectId = ForeignKey(School)
    address: Address = mongoz.EmbeddedDocument()

    class Meta:
        registry = registry
        database = "test_database"

    @staticmethod
    def get_dbtype():
        return "students"

