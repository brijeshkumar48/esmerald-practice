import re
from typing import Union, ClassVar
from bson import ObjectId
from mongoz import Document

class MethodNotDefined(Exception):
    """Raised when a required method is not defined in a subclass."""
    def __init__(self, detail: str):
        super().__init__(detail)

class BaseDocument(Document):
    """
    The base document for all documents using the 'registry' registry.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Check if the child class has the 'get_dbtype' method
        if not hasattr(self, "get_dbtype") or not callable(getattr(self, "get_dbtype", None)):
            raise MethodNotDefined(
                detail=f"Method get_dbtype is not defined in class {self.__class__.__name__}"
            )

    @classmethod
    def convert_str_to_object_id(cls, id: Union[ObjectId, str]) -> ObjectId:
        """
        Return the ObjectId by formatting and verifying the input ID.
        """
        if isinstance(id, str):
            m = re.search(r'ObjectId\([\'"]([a-fA-F0-9]{24})[\'"]\)', id.strip())
            if m:
                object_id_value = m.group(1)
                return ObjectId(object_id_value)
            return ObjectId(id)
        raise ValueError(
            "pass the valid object id, where the provided id is"
            f"{id} and type of id is {type(id).__name__}"
        )
