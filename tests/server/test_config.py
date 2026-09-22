from baski.server.config import Config


def test_dotted_write_into_an_existing_branch():
    cfg = Config({"a": {"x": 1}})
    cfg["a.b"] = 2
    assert cfg["a.b"] == 2
    assert cfg["a.x"] == 1


def test_dotted_write_creates_the_missing_branch():
    cfg = Config({"a": {"x": 1}})
    cfg["z.q"] = 3
    assert cfg["z.q"] == 3
    assert "z" in cfg


def test_dotted_write_three_levels_deep():
    cfg = Config()
    cfg["feature.flag.enabled"] = True
    assert cfg["feature.flag.enabled"] is True
    assert cfg["feature"]["flag"]["enabled"] is True


def test_created_branch_is_a_config_not_a_plain_dict():
    cfg = Config()
    cfg["a.leaf"] = 1
    branch = cfg["a"]
    assert branch["leaf"] == 1
    assert branch["absent"] == Config(), "a created level must keep the lazy missing-key behaviour"
