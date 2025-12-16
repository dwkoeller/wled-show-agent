from __future__ import annotations

from xlights_import import parse_xlights_networks_xml


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

