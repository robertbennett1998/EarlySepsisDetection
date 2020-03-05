import hpo
import os
import tensorflow as tf

#Starting with SepDFN100 configuration
DFFNN = [
    #0 - Dense - Input
    hpo.Layer(layer_name="input_layer_dense", layer_type=tf.keras.layers.Dense,
    parameters=[
        hpo.Parameter(parameter_name="units", parameter_value=30)
    ],
    hyperparameters=[
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),
    #1 - Dense
    hpo.Layer(layer_name="hidden_layer_dense_1", layer_type=tf.keras.layers.Dense,
    parameters=[],
    hyperparameters=[
        hpo.Parameter(parameter_name="units", parameter_value=64, value_range=[2 ** x for x in range(0, 13)]),
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),
    #2 - Dense
    hpo.Layer(layer_name="hidden_layer_dense_2", layer_type=tf.keras.layers.Dense,
    parameters=[],
    hyperparameters=[
        hpo.Parameter(parameter_name="units", parameter_value=48, value_range=[2 ** x for x in range(0, 13)]),
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),
    #3 - Dense
    hpo.Layer(layer_name="hidden_layer_dense_3", layer_type=tf.keras.layers.Dense,
    parameters=[],
    hyperparameters=[
        hpo.Parameter(parameter_name="units", parameter_value=24, value_range=[2 ** x for x in range(0, 13)]),
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),
    #4 - Dense
    hpo.Layer(layer_name="hidden_layer_dense_4", layer_type=tf.keras.layers.Dense,
    parameters=[],
    hyperparameters=[
        hpo.Parameter(parameter_name="units", parameter_value=24, value_range=[2 ** x for x in range(0, 13)]),
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),
    #5 - Dense - Output
    hpo.Layer(layer_name="hidden_layer_dense_4", layer_type=tf.keras.layers.Dense,
    parameters=[
        hpo.Parameter(parameter_name="units", parameter_value=1),
    ],
    hyperparameters=[
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ])
]

LTSM_RNN = None #[
#     #0 - Dense - Input
#     hpo.Layer(layer_name="input_layer_dense", layer_type=tf.keras.layers.Dense,
#     parameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=100)
#         hpo.Parameter(parameter_name="input_shape", parameter_value=(200, 200, 3))
#     ],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ]),
#     #1 - Dense
#     hpo.Layer(layer_name="hidden_layer_dense_1", layer_type=tf.keras.layers.Dense,
#     parameters=[],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=64, value_range=[x for x in range(0, 256)]),
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ]),
#     #2 - Dense
#     hpo.Layer(layer_name="hidden_layer_dense_2", layer_type=tf.keras.layers.Dense,
#     parameters=[],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=48, value_range=[x for x in range(0, 256)]),
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ]),
#     #3 - Dense
#     hpo.Layer(layer_name="hidden_layer_dense_3", layer_type=tf.keras.layers.Dense,
#     parameters=[],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=24, value_range=[x for x in range(0, 256)]),
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ]),
#     #4 - Dense
#     hpo.Layer(layer_name="hidden_layer_dense_4", layer_type=tf.keras.layers.Dense,
#     parameters=[],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=24, value_range=[x for x in range(0, 256)]),
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ]),
#     #5 - Dense - Output
#     hpo.Layer(layer_name="hidden_layer_dense_4", layer_type=tf.keras.layers.Dense,
#     parameters=[
#         hpo.Parameter(parameter_name="units", parameter_value=2),
#     ],
#     hyperparameters=[
#         hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
#     ])
# ]