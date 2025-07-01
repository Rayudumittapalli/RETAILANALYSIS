import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_generic_orders
from lib.ConfigReader import get_app_config

@pytest.mark.skip()
def test_read_customers_df(spark):
    customers_df=read_customers(spark,"LOCAL")
    cust_cnt=customers_df.count()
    assert cust_cnt==12435

@pytest.mark.skip()
def test_read_orders_df(spark):
    
    orders_df=read_orders(spark,"LOCAL")
    orders_cnt=orders_df.count()
    assert orders_cnt==68883

@pytest.mark.skip()
def test_filter_orders_df(spark):
    
    orders_df=read_orders(spark,"LOCAL")
    closed_cnt=filter_closed_orders(orders_df).count()
    assert closed_cnt==7556

@pytest.mark.skip()
def test_read_app_config(spark):
    
    config=get_app_config("LOCAL")
    assert config["customers.file.path"]=="data/customers.csv"
    assert config["orders.file.path"]=="data/orders.csv"

@pytest.mark.skip()
def test_count_orders_state(spark,expected_results):
    customers_df=read_customers(spark,"LOCAL")
    agg_result=count_orders_state(customers_df)
    assert agg_result.collect()==expected_results.collect()

@pytest.mark.skip()
def test_filter_orders_CLOSED_df(spark):
    orders_df=read_orders(spark,"LOCAL")
    orders_closed_cnt=filter_generic_orders(orders_df,"CLOSED").count()
    assert orders_closed_cnt==7556


@pytest.mark.skip()
def test_filter_orders_PENDING_PAYMENT_df(spark):
    orders_df=read_orders(spark,"LOCAL")
    orders_PP_cnt=filter_generic_orders(orders_df,"PENDING_PAYMENT").count()
    assert orders_PP_cnt==15030

@pytest.mark.skip()
def test_filter_orders_COMPLETE_df(spark):
    orders_df=read_orders(spark,"LOCAL")
    orders_COMP_cnt=filter_generic_orders(orders_df,"COMPLETE").count()
    assert orders_COMP_cnt==22899

@pytest.mark.parametrize(
        "status,count",
        [ 
            ("CLOSED",7556),
            ("PENDING_PAYMENT",15030),
            ("COMPLETE",22899)
        ]
)

def test_filter_orders_generic(spark,status,count):
    orders_df=read_orders(spark,"LOCAL")
    orders_status_cnt=filter_generic_orders(orders_df,status).count()
    assert orders_status_cnt==count