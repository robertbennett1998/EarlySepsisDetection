import hpo
import os

import numpy as np
import tensorflow as tf

#Starting with SepDFN100 configuration
DFFNN = [

    #0 - Dense - Input
    hpo.Layer(layer_name="input_layer_dense", layer_type=tf.keras.layers.Dense,
    parameters=[
        hpo.Parameter(parameter_name="units", parameter_value=30),
        hpo.Parameter(parameter_name="input_shape", parameter_value=(5, 30))
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

CNN = [
    hpo.Layer(layer_name="input_layer_conv1d", layer_type=tf.keras.layers.Conv1D,
    parameters=[
        hpo.Parameter(parameter_name="input_shape", parameter_value=(5, 30))
    ],
    hyperparameters=[
        hpo.Parameter(parameter_name="filters", parameter_value=8, value_range=[2 ** x for x in range(0, 5)], constraints=None),
        hpo.Parameter(parameter_name="kernel_size", parameter_value=3, value_range=[1, 2, 3, 4, 5], constraints=None),
        hpo.Parameter(parameter_name="activation", parameter_value="relu", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),

    hpo.Layer(layer_name="hidden_layer_flatten_1", layer_type=tf.keras.layers.Flatten,
    parameters=[
    ],
    hyperparameters=[
    ]),

    hpo.Layer(layer_name="hidden_layer_dense_1", layer_type=tf.keras.layers.Dense,
    parameters=[],
    hyperparameters=[
        hpo.Parameter(parameter_name="units", parameter_value=64, value_range=[2 ** x for x in range(0, 13)]),
        hpo.Parameter(parameter_name="activation", parameter_value="relu", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ]),

    hpo.Layer(layer_name="hidden_layer_dense_4", layer_type=tf.keras.layers.Dense,
    parameters=[
        hpo.Parameter(parameter_name="units", parameter_value=1),
    ],
    hyperparameters=[
        hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None)
    ])
]

LTSM_RNN = [
    hpo.Layer(layer_name="hidden_layer_3_dense", layer_type=tf.keras.layers.LSTM,
              parameters=[ hpo.Parameter(parameter_name="return_sequences", parameter_value=True)],
              hyperparameters=[
                  hpo.Parameter(parameter_name="activation", parameter_value="sigmoid",
                                value_range=["tanh", "sigmoid", "relu"], constraints=None, encode_string_values=True),
                  # need to add more
                  hpo.Parameter(parameter_name="units", parameter_value=128, value_range=[2 ** x for x in range(7, 13)],
                                constraints=None),
                  hpo.Parameter(parameter_name="use_bias", parameter_value=1, value_range=[1, 0], constraints=None)
              ]),

    hpo.Layer(layer_name="input_layer_lstm", layer_type=tf.keras.layers.LSTM,
        parameters=[
            hpo.Parameter(parameter_name="input_shape", parameter_value=(5, 30)),
            hpo.Parameter(parameter_name="return_sequences", parameter_value=False)
        ],
        hyperparameters=[
            hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["tanh", "sigmoid", "relu"], constraints=None, encode_string_values=True),#need to add more
            hpo.Parameter(parameter_name="units", parameter_value=64, value_range=[2**x for x in range(7, 13)], constraints=None),
            hpo.Parameter(parameter_name="use_bias", parameter_value=1, value_range=[1, 0], constraints=None, encode_string_values=True)
        ]),

    # hpo.Layer(layer_name="hidden_layer_1_lstm", layer_type=tf.keras.layers.LSTM,
    #           parameters=[
    #               hpo.Parameter(parameter_name="input_shape", parameter_value=(5, 30)),
    #               hpo.Parameter(parameter_name="return_sequences", parameter_value=True)
    #           ],
    #           hyperparameters=[
    #               hpo.Parameter(parameter_name="activation", parameter_value="relu",
    #                             value_range=["tanh", "sigmoid", "relu"], constraints=None, encode_string_values=True),
    #               # need to add more
    #               hpo.Parameter(parameter_name="units", parameter_value=64, value_range=[2 ** x for x in range(7, 13)],
    #                             constraints=None),
    #               hpo.Parameter(parameter_name="use_bias", parameter_value=1, value_range=[1, 0], constraints=None,
    #                             encode_string_values=True)
    #           ]),
    #
    # hpo.Layer(layer_name="hidden_layer_2_dropout", layer_type=tf.keras.layers.Dropout,
    #       parameters=[
    #           #hpo.Parameter(parameter_name="seed", parameter_value=42)
    #       ],
    #       hyperparameters=[
    #           hpo.Parameter(parameter_name="rate", parameter_value=0.1,
    #                         value_range=np.arange(0.0, 0.4, 0.05).tolist(), constraints=None)
    #       ]),
    #
    hpo.Layer(layer_name="hidden_layer_3_dense", layer_type=tf.keras.layers.Dense,
        parameters=[],
        hyperparameters=[
            hpo.Parameter(parameter_name="activation", parameter_value="relu", value_range=["tanh", "sigmoid", "relu"], constraints=None, encode_string_values=True),#need to add more
            hpo.Parameter(parameter_name="units", parameter_value=32, value_range=[2**x for x in range(7, 13)], constraints=None),
            hpo.Parameter(parameter_name="use_bias", parameter_value=1, value_range=[1, 0], constraints=None)
        ]),
    #
    # hpo.Layer(layer_name="hidden_layer_4_dropout", layer_type=tf.keras.layers.Dropout,
    #       parameters=[
    #           hpo.Parameter(parameter_name="seed", parameter_value=42)
    #       ],
    #       hyperparameters=[
    #           hpo.Parameter(parameter_name="rate", parameter_value=0.1,
    #                         value_range=np.arange(0.0, 0.4, 0.05).tolist(), constraints=None)
    #       ]),

    hpo.Layer(layer_name="output_layer_dense", layer_type=tf.keras.layers.Dense,
        parameters=[
            hpo.Parameter(parameter_name="units", parameter_value=1),
        ],
        hyperparameters=[
            hpo.Parameter(parameter_name="activation", parameter_value="sigmoid", value_range=["relu", "tanh", "sigmoid"], constraints=None, encode_string_values=True)
        ])
]