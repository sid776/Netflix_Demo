import React from 'react';
import { motion } from 'framer-motion';
import MLVisualization from '../components/MLVisualization';

const RecommendationSystem: React.FC = () => {
  return (
    <div className="container mx-auto px-4 py-8">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8 }}
        className="max-w-4xl mx-auto"
      >
        <h1 className="text-4xl font-bold mb-6">Netflix Recommendation System</h1>
        
        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">How It Works</h2>
          <p className="text-gray-300 mb-4">
            Netflix's recommendation system uses deep learning algorithms to analyze your viewing patterns,
            preferences, and behavior to suggest content you're likely to enjoy.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
            <FeatureBox
              title="Content Analysis"
              description="Deep neural networks analyze video content, metadata, and user interactions"
            />
            <FeatureBox
              title="Personalization"
              description="Machine learning models create unique user profiles and preferences"
            />
            <FeatureBox
              title="Real-time Learning"
              description="Continuous adaptation to user behavior and feedback"
            />
            <FeatureBox
              title="A/B Testing"
              description="Constant experimentation to improve recommendation accuracy"
            />
          </div>
        </section>

        <section className="mb-12">
          <MLVisualization />
        </section>

        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Technical Implementation</h2>
          <div className="bg-gray-800 rounded-lg p-6">
            <ul className="list-disc list-inside space-y-2 text-gray-300">
              <li>Neural Collaborative Filtering (NCF) for user-item interactions</li>
              <li>Convolutional Neural Networks (CNN) for video content analysis</li>
              <li>Recurrent Neural Networks (RNN) for sequential pattern recognition</li>
              <li>Transformer models for understanding context and relationships</li>
            </ul>
          </div>
        </section>
      </motion.div>
    </div>
  );
};

const FeatureBox: React.FC<{ title: string; description: string }> = ({
  title,
  description,
}) => {
  return (
    <motion.div
      whileHover={{ scale: 1.02 }}
      className="bg-gray-800 rounded-lg p-4"
    >
      <h3 className="text-xl font-semibold mb-2">{title}</h3>
      <p className="text-gray-300">{description}</p>
    </motion.div>
  );
};

export default RecommendationSystem; 