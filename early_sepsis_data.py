import tensorflow as tf
import os
import pandas as pd
import hpo
import numpy as np

class EarlySepsisData(hpo.Data):
    def __init__(self, data_path, cache_path, training_batch_size, validation_batch_size, test_batch_size):
        super().__init__()

        self._data_path = data_path
        self._cache_path = cache_path
        self._training_batch_size = training_batch_size
        self._validation_batch_size = validation_batch_size
        self._test_batch_size = test_batch_size

        self._training_hospital_admission_count = 0
        self._validation_hospital_admission_count = 0
        self._test_hospital_admission_count = 0

        self._training_data = None
        self._validation_data = None
        self._test_data = None

    def load(self):
        def prepare_dataset(dataset, batch_size, cache=True, repeat=True, prefetch=True, shuffle=True, shuffle_seed=42, shuffle_buffer_size=1000):
            if cache:
                if isinstance(cache, str):
                    print("Opening cache or creating (%s)." % cache)
                    dataset = dataset.cache(cache)
                else:
                    print("No cache path provided. Loading into memory.")
                    dataset = dataset.cache()
            else:
                print("Not caching data. This may be slow.")

            if shuffle:
                dataset = dataset.shuffle(buffer_size=shuffle_buffer_size, seed=shuffle_seed)

            dataset = dataset.repeat()

            if batch_size > 0:
                dataset = dataset.batch(batch_size)

            if prefetch:
                dataset = dataset.prefetch(buffer_size=tf.data.experimental.AUTOTUNE)

            return dataset
        data = pd.read_csv(self._data_path)

        hospital_admission_count = data["hadm_id"].nunique()
        self._training_hospital_admission_count = int(hospital_admission_count * .7)
        self._validation_hospital_admission_count = int(hospital_admission_count * .15)
        self._test_hospital_admission_count = int(hospital_admission_count * .15)

        feature_columns = ["heart_rate_min", "heart_rate_mean",
                           "heart_rate_max", "body_temp_min",
                           "body_temp_mean", "body_temp_max",
                           "respiratory_rate_min", "respiratory_rate_mean",
                           "respiratory_rate_max", "paco2_min",
                           "paco2_mean", "paco2_max",
                           "blood_oxygen_saturation_min", "blood_oxygen_saturation_mean",
                           "blood_oxygen_saturation_max", "systolic_blood_pressure_min",
                           "systolic_blood_pressure_mean", "systolic_blood_pressure_max",
                           "diastolic_blood_pressure_min", "diastolic_blood_pressure_mean",
                           "diastolic_blood_pressure_max", "white_blood_cells_k_per_uL_min",
                           "white_blood_cells_k_per_uL_mean", "white_blood_cells_k_per_uL_max",
                           "immature_bands_percentage_min", "immature_bands_percentage_mean",
                           "immature_bands_percentage_max", "blood_ph_min",
                           "blood_ph_mean", "blood_ph_max"]

        print(data[feature_columns].shape, " - ", data["acquired_sepsis"].shape)

        data = tf.data.Dataset.from_tensor_slices((data[feature_columns].values, data["acquired_sepsis"].values))

        for x in data.take(1):
            print(x)

        self._training_data = data.take(self._training_hospital_admission_count)
        self._test_data = data.skip(self._training_hospital_admission_count)
        self._validation_data = self._test_data.skip(self._training_hospital_admission_count)
        self._test_data = self._test_data.take(self._test_hospital_admission_count)

        self._training_data = prepare_dataset(self._training_data, self._training_batch_size, cache=os.path.join(self._cache_path, "training_images.tfcache"))
        self._validation_data = prepare_dataset(self._validation_data, self._validation_batch_size, cache=os.path.join(self._cache_path, "training_images.tfcache"))
        self._test_data = prepare_dataset(self._test_data, self._test_batch_size, cache=os.path.join(self._cache_path, "training_images.tfcache"))

    def training_steps(self):
        return self._training_hospital_admission_count // self._training_batch_size

    def validation_steps(self):
        return self._validation_hospital_admission_count // self._validation_batch_size

    def test_steps(self):
        return self._test_hospital_admission_count // self._test_batch_size

    def training_data(self):
        return self._training_data

    def validation_data(self):
        return self._validation_data

    def test_data(self):
        return self._test_data