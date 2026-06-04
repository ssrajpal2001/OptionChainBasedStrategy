from data_layer.runtime_config import RuntimeConfig
def test_pool_depths_present_in_default():
    ss = RuntimeConfig.index_section("NIFTY", "sell_straddle")
    assert "pool_itm_depth" in ss and "pool_otm_depth" in ss
    assert int(ss["pool_itm_depth"]) >= 0 and int(ss["pool_otm_depth"]) >= 0
