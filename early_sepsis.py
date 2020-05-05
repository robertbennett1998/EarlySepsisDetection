import early_sepsis_models
import early_sepsis_data
import hpo
import hpo.strategies.bayesian_method
import ray
import os
import tensorflow as tf


ray.init()

if not os.path.exists(os.path.join(os.getcwd(), ".cache")):
    os.mkdir(os.path.join(os.getcwd(), ".cache"))

optimiser = hpo.Optimiser(optimiser_name="optimiser_adam", optimiser_type=tf.keras.optimizers.Adam, hyperparameters=[
    hpo.Parameter(parameter_name="learning_rate", parameter_value=0.0000001, value_range=[1 * (10 ** n) for n in range(0, -7, -1)])
])

model_configuration = hpo.DefaulDLModelConfiguration(optimiser=optimiser, layers=early_sepsis_models.LTSM_RNN, loss_function="binary_crossentropy", number_of_epochs=30)

data_path = os.path.join(os.getcwd(), "../../data/early_sepsis/sepsis_patients.csv")


def construct_early_sepsis_data():
    return early_sepsis_data.EarlySepsisData(data_path, os.path.join(os.getcwd(), ".cache"), 20, 20, 20)


def construct_chromosome():
    return hpo.strategies.genetic_algorithm.DefaultChromosome(model_configuration)


strategy = hpo.strategies.bayesian_method.BayesianMethod(model_configuration, 1, hpo.strategies.bayesian_method.RandomForestSurrogate())
hpo_instance = hpo.Hpo(model_configuration, construct_early_sepsis_data, strategy)
results = hpo_instance.execute()
results.save(os.path.join(os.getcwd(), "early_sepsis.results"))

results.best_result().plot_train_val_accuracy()
results.best_result().plot_train_val_loss()