from google.cloud import firestore
from dataclasses import is_dataclass, fields


def from_doc(klass, doc: firestore.DocumentSnapshot):
    assert is_dataclass(klass), "klass must be a dataclass"
    klass_fields = {f.name for f in fields(klass)}
    data = {k: v for k, v in doc.to_dict().items() if k in klass_fields}
    return klass(**data)
