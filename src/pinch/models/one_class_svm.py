#! /usr/bin/env/python3

import os
import pickle as pkl

from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler


class SVMClassifier:
    """
    A one-class SVM classifier with preprocessing and feature engineering.

    This class supports training and evaluating a one-class SVM using features derived
    from pipeline triggers. It also handles saving/loading models with their scalers.

    Attributes:
        cutoff_params (list[str]): Parameters used as SVM inputs.
        model (OneClassSVM): Trained scikit-learn OneClassSVM model.
        scaler (StandardScaler): Scaler used to normalize feature data.

    Methods:
        compute_training_params(df, param): Compute derived training features.
        apply_feature_engineering(df): Add engineered features to DataFrame if missing.
        train_model(training_df, n_samples): Train the one-class SVM.
        evaluate(df): Score a new DataFrame using the trained SVM.
        save_model(path): Save model and scaler to a file.
        load_model(path, cutoff_params): Load model and scaler from file.
        train_from_data(train_df, cutoff_params, n_samples): Train model and return instance.
    """
    def __init__(
            self,
            cutoff_params=None,
            model=None,
            scaler=None,
            ):

        self.cutoff_params = cutoff_params or ['snr', 'chisqBysnrsq']
        self.model = model
        self.scaler = scaler or StandardScaler()

    def compute_training_params(self, df, param):
        """
        Compute a derived parameter for feature engineering.

        Args:
            df (pd.DataFrame): Input DataFrame.
            param (str): Parameter to compute. Currently supports 'chisqBysnrsq'.

        Returns:
            pd.Series: The computed parameter series.

        Raises:
            ValueError: If the parameter is not supported.
        """
        if param == 'chisqBysnrsq':
            return df['chisq'] / df['snr']**2
        raise ValueError(f"Unsupported param for training param: {param}")

    def apply_feature_engineering(self, df):
        """
        Ensure required features are present in the DataFrame.

        If any features in `self.cutoff_params` are missing, they are computed.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with required features added.
        """
        for param in self.cutoff_params:
            if param not in df.columns:
                df[param] = self.compute_training_params(df, param)

        return df

    def train_model(self, training_df, n_samples=10000):
        """
        Train a one-class SVM model on a sample of the training data.

        Args:
            training_df (pd.DataFrame): DataFrame containing training examples.
            n_samples (int): Number of samples to randomly select for training.
        """
        training_df = self.apply_feature_engineering(training_df)

        if n_samples < len(training_df):
            sampled = training_df[self.cutoff_params].sample(n=n_samples)

        else:
            sampled = training_df[self.cutoff_params].sample(n=n_samples, replace=True)

        scaled_clean = self.scaler.fit_transform(sampled)

        #FIXME make nu an argument
        self.model = OneClassSVM(kernel="rbf", nu=0.01).fit(scaled_clean)

    def evaluate(self, df):
        """
        Evaluate input data using the trained model.

        Args:
            df (pd.DataFrame): DataFrame to score.

        Returns:
            pd.DataFrame: Copy of input DataFrame with new `svm_score` column.

        Raises:
            RuntimeError: If model or scaler is not initialized.
        """
        if self.model is None or self.scaler is None:
            raise RuntimeError("Model and scaler must be set before evaluation")

        df = self.apply_feature_engineering(df)
        data = self.scaler.transform(
                df[self.cutoff_params].values
            )

        scores = -self.model.decision_function(data)
        df = df.copy()
        df.loc[:, 'svm_score'] = scores

        return df

    def save_model(self, path):
        """
        Save the trained model and scaler to disk using pickle.

        Args:
            path (str): Path to save the model file.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'wb') as f:
            pkl.dump({
                'model': self.model,
                'scaler': self.scaler},
                f
            )

    @classmethod
    def load_model(cls, path, cutoff_params=None):
        """
        Load a model and scaler from a pickle file.

        Args:
            path (str): Path to the pickle file.
            cutoff_params (list[str], optional): Parameters used during training.

        Returns:
            SVMClassifier: An instance with loaded model and scaler.
        """
        with open(path, 'rb') as f:
            payload = pkl.load(f)

        return cls(
                cutoff_params=cutoff_params,
                model=payload['model'],
                scaler=payload['scaler'],
            )

    @classmethod
    def train_from_data(cls, train_df, cutoff_params=None, n_samples=10000):
        """
        Train a model from a given DataFrame and return the classifier instance.

        Args:
            train_df (pd.DataFrame): DataFrame to train on.
            cutoff_params (list[str], optional): Parameters to train with.
            n_samples (int): Number of rows to sample from the training data.

        Returns:
            SVMClassifier: A trained classifier instance.
        """
        instance = cls(cutoff_params=cutoff_params)
        instance.train_model(train_df, n_samples=n_samples)
        return instance
