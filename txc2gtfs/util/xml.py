from typing import TYPE_CHECKING, overload


if TYPE_CHECKING:
    import xml.etree.ElementTree as ET


type XMLElement = ET.Element[str]
type XMLTree = ET.ElementTree[XMLElement]

NS = {"txc": "http://www.transxchange.org.uk/"}


@overload
def get_text[T](base: XMLElement, path: str, *, default: T) -> str | T: ...


@overload
def get_text(base: XMLElement, path: str) -> str: ...


def get_text[T](base: XMLElement, path: str, **kwargs: T) -> str | T:
    el = base.find(path, NS)
    if "default" in kwargs and el is None:
        return kwargs["default"]
    assert el is not None
    text = el.text
    if "default" in kwargs:
        return text or kwargs["default"]
    assert text
    return text
