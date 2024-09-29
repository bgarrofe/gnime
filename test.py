from pipefunc import pipefunc, Pipeline


@pipefunc(output_name="b")
def f_c():
    return 1


@pipefunc(output_name="c")
def f_d(b):
    return b + 1


pipeline = Pipeline([f_c, f_d], profile=True)
print(pipeline())
