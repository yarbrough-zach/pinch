#!/usr/bin/env python3

import os
import pandas as pd

from typing import Optional, Union
from pathlib import Path
import logging

from pinch.models.one_class_svm import SVMClassifier

logger = logging.getLogger(__name__)


class SVMPipeline:
    """
    A pipeline to train and apply a one-class SVM model to glitch trigger data.

    This class can operate in three modes:
        - "train": Train a model on clean triggers.
        - "score": Apply a trained model to dirty triggers.
        - "train_and_score": Train a model and immediately apply it.

    Attributes:
        trainer (SVMClassifier or None): The trained SVM model.
        clean_df (pd.DataFrame or None): Clean glitch triggers for training.
        dirty_df (pd.DataFrame or None): Dirty glitch triggers for scoring.
        scored_df (pd.DataFrame or None): Scored triggers after evaluation.
    """
    def __init__(
            self,
            clean_df: Optional[pd.DataFrame] = None,
            dirty_df: Optional[pd.DataFrame] = None,
            trainer: Optional[SVMClassifier] = None,
            model_path: Optional[str | Path] = None,
            output_path: Optional[str | Path] = None,
    ) -> None:
        self.clean_df = clean_df
        self.dirty_df = dirty_df
        self.trainer = trainer
        self.model_path = model_path
        self.output_path = output_path
        self.scored_df = None

    def train(self, save_model: bool = False) -> None:
        """
        Train a one-class SVM model on clean data.

        Optionally saves the trained model to a file.
        """
        if self.clean_df is None:
            msg = "No clean dataframe provided for training"
            logger.error(msg)
            raise ValueError(msg)

        self.trainer = SVMClassifier.train_from_data(self.clean_df)

        if save_model:
            if not self.model_path:
                msg = "No model_path specified to save the model"
                logger.error(msg)
                raise ValueError(msg)

            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            self.trainer.save_model(self.model_path)

    def evaluate(self) -> pd.DataFrame:
        """
        Score dirty triggers using the trained SVM model.

        Raises:
            ValueError: If no model is provided or trained.
        """
        if self.dirty_df is None:
            msg = "No dirty dataframe provided for evaluation"
            logger.error(msg)
            raise ValueError(msg)

        if self.trainer is None:
            if not self.model_path:
                msg = "No model speficied for evaluation"
                logger.error(msg)
                raise ValueError(msg)

            self.trainer = SVMClassifier.load_model(self.model_path)

        scored_df = self.trainer.evaluate(self.dirty_df)

        self.scored_df = scored_df

        return self.scored_df

    def save_scored_data(self) -> None:
        """
        Save scored dirty triggers to the specified output path.

        Raises:
            ValueError: If no scored data is available.
        """
        if self.scored_df is None:
            msg = "No scored data available, did you forget to call evaluate()?"
            logger.error(msg)
            raise ValueError(msg)

        if not self.output_path:
            msg = "No output_path specified to save scored data"
            logger.error(msg)
            raise ValueError(msg)

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self._write_scored_df(self.scored_df, self.output_path)

    @staticmethod
    def _load_trigger_file(path: str | Path) -> None:
        return pd.read_csv(path)

    @staticmethod
    def _write_scored_df(df, path: str | Path) -> None:
        df.to_csv(f"{path}/scored_df.csv", index=False)
