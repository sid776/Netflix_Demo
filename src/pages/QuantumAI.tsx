import React from 'react';
import { motion } from 'framer-motion';

const QuantumAI: React.FC = () => {
  return (
    <div className="container mx-auto px-4 py-8">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.8 }}
        className="max-w-4xl mx-auto"
      >
        <h1 className="text-4xl font-bold mb-6">Quantum AI at Netflix</h1>
        
        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">The Future of Streaming</h2>
          <p className="text-gray-300 mb-4">
            Netflix is exploring quantum computing applications to revolutionize content delivery,
            recommendation systems, and optimization algorithms. Quantum AI promises to solve
            complex problems that are currently intractable for classical computers.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
            <FeatureBox
              title="Quantum Machine Learning"
              description="Leveraging quantum algorithms for enhanced pattern recognition and optimization"
            />
            <FeatureBox
              title="Quantum Optimization"
              description="Using quantum computing to solve complex optimization problems in real-time"
            />
            <FeatureBox
              title="Quantum Security"
              description="Implementing quantum-resistant encryption for enhanced content protection"
            />
            <FeatureBox
              title="Quantum Simulation"
              description="Simulating complex systems for improved content delivery and user experience"
            />
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Technical Implementation</h2>
          <div className="bg-gray-800 rounded-lg p-6">
            <ul className="list-disc list-inside space-y-2 text-gray-300">
              <li>Quantum Neural Networks for enhanced pattern recognition</li>
              <li>Quantum Annealing for optimization problems</li>
              <li>Quantum Key Distribution for secure content delivery</li>
              <li>Quantum Circuit Design for custom algorithms</li>
            </ul>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-2xl font-semibold mb-4">Potential Impact</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <ImpactCard
              title="Faster Processing"
              description="Exponential speedup in complex computations"
            />
            <ImpactCard
              title="Better Predictions"
              description="Enhanced accuracy in content recommendations"
            />
            <ImpactCard
              title="Secure Delivery"
              description="Quantum-resistant security measures"
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

const ImpactCard: React.FC<{ title: string; description: string }> = ({
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

export default QuantumAI; 