from pipefunc import Pipeline, PipeFunc


def f_a():
    return 1


def f_b():
    return 2


def f_c():
    return 3


def f_d(input_1, input_2, input_3):
    return input_1 + input_2 + input_3


fa = PipeFunc(
    f_a,
    output_name="node_1_output_0"
)
fb = PipeFunc(
    f_b,
    output_name="node_2_output_0"
)
fc = PipeFunc(
    f_c,
    output_name="node_3_output_0"
)
fd = PipeFunc(
    f_d,
    output_name="node_4_output_0",
    renames={
        "input_1": "node_1_output_0",
        "input_2": "node_2_output_0",
        "input_3": "node_3_output_0"
    }
)

pipeline = Pipeline([fa, fb, fc, fd], profile=True)
print(pipeline())
