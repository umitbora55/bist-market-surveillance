"""
LSTM Autoencoder for Anomaly Detection
Detects anomalous trading patterns using reconstruction error
"""

import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers, Model
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
import matplotlib.pyplot as plt
from datetime import datetime
import os


class LSTMAutoencoder:
    """LSTM Autoencoder for time series anomaly detection"""
    
    def __init__(self, sequence_length=10, n_features=10):
        self.sequence_length = sequence_length
        self.n_features = n_features
        self.model = None
        self.threshold = None
    
    def build_model(self, encoding_dim=32):
        """Build LSTM Autoencoder architecture"""
        
        # Encoder
        encoder_inputs = layers.Input(shape=(self.sequence_length, self.n_features))
        
        # LSTM layers with dropout
        x = layers.LSTM(64, return_sequences=True)(encoder_inputs)
        x = layers.Dropout(0.2)(x)
        x = layers.LSTM(encoding_dim, return_sequences=False)(x)
        
        encoder = Model(encoder_inputs, x, name='encoder')
        
        # Decoder
        decoder_inputs = layers.Input(shape=(encoding_dim,))
        
        # Repeat vector to match sequence length
        x = layers.RepeatVector(self.sequence_length)(decoder_inputs)
        
        # LSTM layers
        x = layers.LSTM(encoding_dim, return_sequences=True)(x)
        x = layers.Dropout(0.2)(x)
        x = layers.LSTM(64, return_sequences=True)(x)
        
        # Output layer
        decoder_outputs = layers.TimeDistributed(
            layers.Dense(self.n_features)
        )(x)
        
        decoder = Model(decoder_inputs, decoder_outputs, name='decoder')
        
        # Autoencoder (encoder + decoder)
        autoencoder_inputs = layers.Input(shape=(self.sequence_length, self.n_features))
        encoded = encoder(autoencoder_inputs)
        decoded = decoder(encoded)
        
        autoencoder = Model(autoencoder_inputs, decoded, name='autoencoder')
        
        # Compile
        autoencoder.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.001),
            loss='mse'
        )
        
        self.model = autoencoder
        self.encoder = encoder
        self.decoder = decoder
        
        return autoencoder
    
    def train(self, X_train, epochs=50, batch_size=32, validation_split=0.2):
        """Train the autoencoder"""
        
        if self.model is None:
            self.build_model()
        
        print("\nTraining LSTM Autoencoder...")
        print(f"Training samples: {len(X_train)}")
        print(f"Input shape: {X_train.shape}")
        
        # Callbacks
        early_stopping = EarlyStopping(
            monitor='val_loss',
            patience=10,
            restore_best_weights=True
        )
        
        # Train
        history = self.model.fit(
            X_train, X_train,  # Autoencoder learns to reconstruct input
            epochs=epochs,
            batch_size=batch_size,
            validation_split=validation_split,
            callbacks=[early_stopping],
            verbose=1
        )
        
        # Calculate reconstruction errors on training data
        reconstructions = self.model.predict(X_train)
        train_errors = np.mean(np.abs(reconstructions - X_train), axis=(1, 2))
        
        # Set threshold (99th percentile of training errors)
        self.threshold = np.percentile(train_errors, 99)
        
        print(f"\nTraining complete!")
        print(f"Anomaly threshold: {self.threshold:.4f}")
        
        return history
    
    def predict_anomaly(self, X):
        """Predict if sequences are anomalous"""
        
        if self.model is None:
            raise ValueError("Model not trained yet!")
        
        # Reconstruct
        reconstructions = self.model.predict(X)
        
        # Calculate reconstruction errors
        errors = np.mean(np.abs(reconstructions - X), axis=(1, 2))
        
        # Classify as anomaly if error > threshold
        anomalies = errors > self.threshold
        
        return anomalies, errors
    
    def save_model(self, filepath='models/lstm_autoencoder.h5'):
        """Save model to disk"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.model.save(filepath)
        
        # Save threshold
        np.save(filepath.replace('.h5', '_threshold.npy'), self.threshold)
        
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath='models/lstm_autoencoder.h5'):
        """Load model from disk"""
        self.model = keras.models.load_model(filepath)
        
        # Load threshold
        self.threshold = np.load(filepath.replace('.h5', '_threshold.npy'))
        
        print(f"Model loaded from {filepath}")
    
    def plot_training_history(self, history):
        """Plot training history"""
        plt.figure(figsize=(10, 4))
        
        plt.plot(history.history['loss'], label='Training Loss')
        plt.plot(history.history['val_loss'], label='Validation Loss')
        plt.title('LSTM Autoencoder Training History')
        plt.xlabel('Epoch')
        plt.ylabel('Loss (MSE)')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('models/training_history.png')
        print("Training history plot saved to models/training_history.png")


def main():
    """Train LSTM Autoencoder"""
    from data_preparation import DataPreparation
    
    # Prepare data
    prep = DataPreparation()
    X, symbols, features = prep.prepare_training_data(sequence_length=10)
    
    if X is None:
        print("Insufficient data for training")
        return
    
    # Initialize model
    model = LSTMAutoencoder(sequence_length=10, n_features=10)
    
    # Train
    history = model.train(X, epochs=50, batch_size=16)
    
    # Save model
    model.save_model('models/lstm_autoencoder.h5')
    
    # Plot history
    model.plot_training_history(history)
    
    # Test anomaly detection
    print("\n" + "="*60)
    print("Testing anomaly detection on training data...")
    anomalies, errors = model.predict_anomaly(X)
    
    print(f"Total samples: {len(X)}")
    print(f"Detected anomalies: {anomalies.sum()}")
    print(f"Anomaly rate: {anomalies.sum() / len(X) * 100:.2f}%")
    print(f"Min error: {errors.min():.4f}")
    print(f"Max error: {errors.max():.4f}")
    print(f"Mean error: {errors.mean():.4f}")
    print(f"Threshold: {model.threshold:.4f}")


if __name__ == "__main__":
    main()
