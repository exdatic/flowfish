import pytest
import simpleeval


def test_max_comprehension_length():
    evaluator = simpleeval.EvalWithCompoundTypes(functions=dict(range=range))
    simpleeval.MAX_COMPREHENSION_LENGTH = 10000
    with pytest.raises(simpleeval.IterableTooLong):
        evaluator.eval("[i for i in range(10001)]")
    simpleeval.MAX_COMPREHENSION_LENGTH = 10001
    evaluator.eval("[i for i in range(10001)]")
