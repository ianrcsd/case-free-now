from operators.custom_operator import HelloOperator


def test_custom_operator():
    test = HelloOperator(task_id="test", name="test_user")
    result = test.execute(context={})
    assert result == "Hello test_user"
