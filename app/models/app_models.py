from mongoz import String, Document, fields
from typing import Annotated, Any, ClassVar, List, Optional, Type, Union
import mongoz
from pydantic import Field
from app.baseModel import BaseDocument
from db.registry import registry
from bson import ObjectId
from mongoz import Document, EmbeddedDocument
from mongoz.core.db.fields.core import FieldFactory, CLASS_DEFAULTS
from mongoz.core.db.fields.base import BaseField


class ForeignKey(FieldFactory, ObjectId):
    _type = ObjectId

    def __new__(
        cls,
        model: Union[Type[Document], Type[EmbeddedDocument]],
        null: bool = False,
        **kwargs: Any,
    ) -> BaseField:
        kwargs = {
            **kwargs,
            **{
                key: value
                for key, value in locals().items()
                if key not in CLASS_DEFAULTS
            },
            # "null": null,
            # "json_schema_extra": {
            #     "Meta": {
            #         "ref": model.__name__,
            #         "ref_model": model,
            #     }
            # },
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


class University(BaseDocument):
    un_name: str = mongoz.String()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "universities"

    # @staticmethod
    # def get_collection_name():
    #     return "universities"


SchoolBoardChoices = (
    ("ST", "State Board"),
    ("CB", "CBSE"),
    ("IC", "ICSC"),
)


class School(BaseDocument):
    name: str = mongoz.String()
    board: str = mongoz.String(choices=SchoolBoardChoices)
    university_id: ObjectId = ForeignKey(University)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "schools"

    # @staticmethod
    # def get_collection_name():
    #     return "schools"


class Continent(BaseDocument):
    continent_name: str = mongoz.String()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "continents"


class Country(BaseDocument):
    country_name: str = mongoz.String()
    continent_id: ObjectId = ForeignKey(Continent)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "countries"

    # @staticmethod
    # def get_collection_name():
    #     return "countries"


class Address(mongoz.EmbeddedDocument):
    village: str = mongoz.String()
    state: str = mongoz.String()
    pincode: str = mongoz.Integer()
    country_id: ObjectId = ForeignKey(Country)

    class Meta:
        registry = registry
        database = "test_database"
        collection = "address"

    # @staticmethod
    # def get_collection_name():
    #     return "address"


class Student(BaseDocument):
    name: str = mongoz.String()
    std: str = mongoz.String()
    school_id: ObjectId = ForeignKey(School)
    address: Address = mongoz.EmbeddedDocument()

    class Meta:
        registry = registry
        database = "test_database"
        collection = "students"

    # @staticmethod
    # def get_collection_name():
    #     return "students"
