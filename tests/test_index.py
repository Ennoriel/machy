from machy import index


def test_haversine():
    assert index.haversine(52.370216, 4.895168, 52.520008,
                           13.404954) == 946.3876221719836
