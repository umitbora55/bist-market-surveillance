"""
Isolation Forest for Anomaly Detection
Detects multivariate anomalies in trading data
"""

import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import os
from datetime import datetime


class IsolationForestDetector:
    """Isolation Forest for multivariate anomaly detection"""
    
    def __init__(self, contamination=0.1, random_state=42):
        """
        Args:
            contamination: Expected proportion of outliers (0.05 = 5%)
            random_state: Random seed for reproducibility
        """
        self.contamination = contamination
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
            max_samples='auto',
            max_features=1.0,
            bootstrap=False,
            n_jobs=-1,
            verbose=0
        )
        self.feature_names = None
    
    def prepare_features(self, X):
        """
        Flatten sequences to single feature vector per sample
        
        Args:
            X: Sequences (samples, time_steps, features)
        
        Returns:
            X_flat: Flattened features (samples, time_steps * features)
        """
        # Flatten time_steps and features
        n_samples = X.shape[0]
        X_flat = X.reshape(n_samples, -1)
        
        return X_flat
    
    def train(self, X):
        """
        Train Isolation Forest
        
        Args:
            X: Training sequences (samples, time_steps, features)
        """
        print("\nTraining Isolation Forest...")
        print(f"Training samples: {len(X)}")
        print(f"Input shape: {X.shape}")
        print(f"Contamination: {self.contamination}")
        
        # Flatten sequences
        X_flat = self.prepare_features(X)
        
        print(f"Flattened shape: {X_flat.shape}")
        
        # Train
        self.model.fit(X_flat)
        
        # Get anomaly scores on training data
        scores = self.model.score_samples(X_flat)
        predictions = self.model.predict(X_flat)
        
        anomalies = (predictions == -1)
        
        print(f"\nTraining complete!")
        print(f"Detected anomalies: {anomalies.sum()}")
        print(f"Anomaly rate: {anomalies.sum() / len(X) * 100:.2f}%")
        print(f"Min score: {scores.min():.4f}")
        print(f"Max score: {scores.max():.4f}")
        print(f"Mean score: {scores.mean():.4f}")
    
    def predict_anomaly(self, X):
        """
        Predict anomalies
        
        Args:
            X: Sequences to predict (samples, time_steps, features)
        
        Returns:
            anomalies: Boolean array (True = anomaly)
            scores: Anomaly scores (lower = more anomalous)
        """
        # Flatten
        X_flat = self.prepare_features(X)
        
        # Predict
        predictions = self.model.predict(X_flat)
        scores = self.model.score_samples(X_flat)
        
        # Convert to boolean (-1 = anomaly, 1 = normal)
        anomalies = (predictions == -1)
        
        return anomalies, scores
    
    def save_model(self, filepath='models/isolation_forest.pkl'):
        """Save model to disk"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        joblib.dump(self.model, filepath)
        
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath='models/isolation_forest.pkl'):
        """Load model from disk"""
        self.model = joblib.load(filepath)
        
        print(f"Model loaded from {filepath}")


def main():
    """Train Isolation Forest"""
    from data_preparation import DataPreparation
    
    # Prepare data
    prep = DataPreparation()
    X, symbols, features = prep.prepare_training_data(sequence_length=10)
    
    if X is None:
        print("Insufficient data for training")
        return
    
    # Initialize model
    model = IsolationForestDetector(contamination=0.1)
    
    # Train
    model.train(X)
    
    # Save model
    model.save_model('models/isolation_forest.pkl')
    
    # Test on some samples
    print("\n" + "="*60)
    print("Testing on sample data...")
    
    # Test on first 10 samples
    test_X = X[:10]
    anomalies, scores = model.predict_anomaly(test_X)
    
    print(f"\nTest samples: {len(test_X)}")
    print(f"Detected anomalies: {anomalies.sum()}")
    
    for i, (is_anomaly, score) in enumerate(zip(anomalies, scores)):
        status = "ANOMALY" if is_anomaly else "Normal"
        print(f"Sample {i}: {status:8} (score: {score:.4f})")


if __name__ == "__main__":
    main()
