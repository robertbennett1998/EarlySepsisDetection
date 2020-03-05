import early_sepsis_models
import early_sepsis_data
import hpo
import hpo.strategies.genetic_algorithm as hpo_strategy_ga
import ray
import os
import tensorflow as tf

ray.init()

if not os.path.exists(os.path.join(os.getcwd(), ".cache")):
    os.mkdir(os.path.join(os.getcwd(), ".cache"))

optimiser = hpo.Optimiser(optimiser_name="optimiser_adam", optimiser_type=tf.keras.optimizers.Adam, hyperparameters=[
    hpo.Parameter(parameter_name="learning_rate", parameter_value=0.001, value_range=[1 * (10 ** n) for n in range(0, -7, -1)])
])

model_configuration = hpo.ModelConfiguration(optimiser=optimiser, layers=early_sepsis_models.DFFNN, number_of_epochs=10)
print(model_configuration.number_of_hyperparameters())
model_configuration.hyperparameter_summary(True)


def construct_early_sepsis_data():
    return early_sepsis_data.EarlySepsisData(os.path.join(os.getcwd(), "../../data/early_sepsis/sepsis_patients.csv"), os.path.join(os.getcwd(), ".cache"), 100, 100, 100)

def construct_chromosome():
    return hpo.strategies.genetic_algorithm.DefaultChromosome(model_configuration)


strategy = hpo_strategy_ga.GeneticAlgorithm(population_size=30, max_iterations=10, chromosome_type=construct_chromosome, survivour_selection_stratergy="roulette")
hpo_instance = hpo.Hpo(model_configuration, construct_early_sepsis_data, strategy)
hpo_instance.execute()