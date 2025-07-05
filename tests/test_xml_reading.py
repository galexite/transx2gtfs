import pytest

from txc2gtfs.data import get_path


@pytest.fixture
def unpacked_data():
    return get_path("test_data_dir")


@pytest.fixture
def packed_data():
    return get_path("test_packed_data")


@pytest.fixture
def nested_data():
    return get_path("test_nested_packed_data")


@pytest.fixture
def dir_with_packed_data():
    return get_path("test_dir_with_packed_data")


def test_reading_from_unpacked_directory(unpacked_data):
    from untangle import Element

    from txc2gtfs.dataio import get_xml_paths, read_unpacked_xml

    xml_paths = get_xml_paths(unpacked_data)

    # All the paths should be strings to xml
    for path in xml_paths:
        assert isinstance(path, str)
        assert path.endswith(".xml")

        # Test reading into untangle object
        data, filesize, name = read_unpacked_xml(path)
        assert isinstance(data, Element)
        assert "TransXChange" in data.__dir__()


def test_reading_from_packed(packed_data):
    from untangle import Element

    from txc2gtfs.dataio import get_xml_paths, read_xml_inside_zip

    xml_paths = get_xml_paths(packed_data)

    # All the paths should be dicts with strings
    for path in xml_paths:
        assert isinstance(path, dict)
        k = next(iter(path.keys()))
        v = next(iter(path.values()))

        # Key should be the XML name
        assert k.endswith(".xml")
        # Value should be the zip file path
        assert v.endswith(".zip")

        # Test reading into untangle object
        data, filesize, name = read_xml_inside_zip(path)
        assert isinstance(data, Element)
        assert "TransXChange" in data.__dir__()


def test_reading_from_nested(nested_data):
    from untangle import Element

    from txc2gtfs.dataio import get_xml_paths, read_xml_inside_nested_zip

    xml_paths = get_xml_paths(nested_data)

    # All the paths should be dicts with strings
    for path in xml_paths:
        assert isinstance(path, dict)
        k = next(iter(path.keys()))
        v = next(iter(path.values()))

        # Key should be the Zip File path
        assert k.endswith(".zip")
        # Value should be a dictionary
        assert isinstance(v, dict)

        # Test reading into untangle object
        data, filesize, name = read_xml_inside_nested_zip(path)
        assert isinstance(data, Element)
        assert "TransXChange" in data.__dir__()
