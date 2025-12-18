from __future__ import annotations

from xlights_import import (
    parse_xlights_models_xml,
    parse_xlights_networks_xml,
    show_config_from_xlights_project,
)


def test_parse_xlights_networks_xml_extracts_controllers() -> None:
    xml = """
    <xLights>
      <Controller Name="StarESP" IP="172.16.200.60" Protocol="E131" StartUniverse="10" PixelCount="50" />
      <Controller name="YardESP" ip="172.16.200.61" protocol="ArtNet" universe="0" nodes="100" />
      <Controller name="IgnoreMe" ip="not-an-ip" protocol="E131" />
    </xLights>
    """
    ctrls = parse_xlights_networks_xml(xml)
    assert len(ctrls) == 2

    star = next(c for c in ctrls if c.host == "172.16.200.60")
    assert star.protocol == "e131"
    assert star.universe_start == 10
    assert star.pixel_count == 50

    yard = next(c for c in ctrls if c.host == "172.16.200.61")
    assert yard.protocol == "artnet"
    assert yard.universe_start == 0
    assert yard.pixel_count == 100


def test_parse_xlights_models_xml_extracts_models() -> None:
    xml = """
    <xLights>
      <Model name="MegaTree" StartChannel="1" ChannelCount="300" />
      <Model name="Bad" StartChannel="0" ChannelCount="300" />
      <Model name="AlsoBad" StartChannel="1" ChannelCount="0" />
    </xLights>
    """
    models = parse_xlights_models_xml(xml)
    assert len(models) == 1
    m = models[0]
    assert m.name == "MegaTree"
    assert m.start_channel == 1
    assert m.channel_count == 300
    assert m.pixel_count == 100


def test_show_config_from_xlights_project_includes_models() -> None:
    networks_xml = """
    <xLights>
      <Controller Name="YardESP" IP="172.16.200.61" Protocol="E131" StartUniverse="1" PixelCount="100" />
    </xLights>
    """
    models_xml = """
    <xLights>
      <Model name="CandyCane" StartChannel="1" ChannelCount="150" />
      <Model name="Tree" StartChannel="151" ChannelCount="300" />
    </xLights>
    """
    ctrls = parse_xlights_networks_xml(networks_xml)
    models = parse_xlights_models_xml(models_xml)
    cfg = show_config_from_xlights_project(
        networks=ctrls,
        models=models,
        show_name="proj",
        include_controllers=True,
        include_models=True,
    )
    kinds = {p.id: p.kind for p in cfg.props}
    assert "controllers" in cfg.groups
    assert "models" in cfg.groups
    assert any(k == "pixel" for k in kinds.values())
    assert any(k == "model" for k in kinds.values())
