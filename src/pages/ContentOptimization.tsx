import React from 'react';
import { motion } from 'framer-motion';
import DLVisualization from '../components/DLVisualization';

const ContentOptimization: React.FC = () => {
  return (
    <div className="container mx-auto px-4 py-8">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8 }}
        className="max-w-4xl mx-auto"
      >
        <h1 className="text-4xl font-bold mb-6">Content Optimization</h1>
        
        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Video Quality Optimization</h2>
          <p className="text-gray-300 mb-4">
            Netflix uses advanced machine learning algorithms to optimize video quality and streaming
            performance, ensuring the best possible viewing experience across different devices and
            network conditions.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
            <FeatureBox
              title="Adaptive Bitrate Streaming"
              description="AI-powered algorithms that adjust video quality in real-time based on network conditions"
            />
            <FeatureBox
              title="Content-Aware Encoding"
              description="Machine learning models that optimize encoding parameters for different types of content"
            />
            <FeatureBox
              title="Perceptual Quality Optimization"
              description="Deep learning models that enhance visual quality while minimizing bandwidth usage"
            />
            <FeatureBox
              title="Network Prediction"
              description="Predictive models that anticipate network conditions to pre-optimize content delivery"
            />
          </div>
        </section>

        <section className="mb-12">
          <DLVisualization />
        </section>

        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Technical Implementation</h2>
          <div className="bg-gray-800 rounded-lg p-6">
            <ul className="list-disc list-inside space-y-2 text-gray-300">
              <li>Deep Reinforcement Learning for adaptive bitrate selection</li>
              <li>Convolutional Neural Networks for video quality assessment</li>
              <li>Time Series Analysis for network condition prediction</li>
              <li>Multi-objective optimization for quality-bandwidth trade-offs</li>
            </ul>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Benefits</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <BenefitCard
              title="Better Quality"
              description="Optimized video quality across all devices"
            />
            <BenefitCard
              title="Lower Bandwidth"
              description="Reduced data usage while maintaining quality"
            />
            <BenefitCard
              title="Smoother Playback"
              description="Minimized buffering and interruptions"
            />
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

const BenefitCard: React.FC<{ title: string; description: string }> = ({
  title,
  description,
}) => {
  return (
    <motion.div
      whileHover={{ scale: 1.05 }}
      className="bg-netflix-red bg-opacity-20 rounded-lg p-4 text-center"
    >
      <h3 className="text-xl font-semibold mb-2">{title}</h3>
      <p className="text-gray-300">{description}</p>
    </motion.div>
  );
};

export default ContentOptimization; 