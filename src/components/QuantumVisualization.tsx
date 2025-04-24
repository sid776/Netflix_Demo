import React, { useState } from 'react';
import { motion } from 'framer-motion';

const QuantumVisualization: React.FC = () => {
  const [isAnimating, setIsAnimating] = useState(false);

  const quantumStates = [
    { name: 'Classical', description: 'Traditional computing uses bits (0 or 1)' },
    { name: 'Quantum', description: 'Quantum computing uses qubits (can be 0 and 1 simultaneously)' },
  ];

  return (
    <div className="p-6 bg-gray-800 rounded-lg">
      <h3 className="text-2xl font-bold mb-4">Quantum Computing Explained</h3>
      
      <div className="mb-6">
        <div className="bg-gray-700 p-4 rounded-lg mb-4">
          <p className="text-gray-300 mb-4">
            Think of quantum computing like having a super-powered calculator that can try many solutions at once. 
            While regular computers work like a light switch (on or off), quantum computers work more like a dimmer 
            switch that can be in multiple states at the same time!
          </p>
          
          <button
            className="bg-netflix-red text-white px-4 py-2 rounded mb-4"
            onClick={() => setIsAnimating(!isAnimating)}
          >
            {isAnimating ? 'Stop Animation' : 'Start Animation'}
          </button>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {quantumStates.map((state, index) => (
              <motion.div
                key={state.name}
                className="bg-gray-800 p-4 rounded-lg"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.2 }}
              >
                <h4 className="text-xl font-semibold mb-2">{state.name}</h4>
                <p className="text-gray-300 mb-4">{state.description}</p>
                <div className="relative h-20">
                  {state.name === 'Classical' ? (
                    <motion.div
                      className="absolute top-0 left-0 w-8 h-8 bg-netflix-red rounded-full"
                      animate={isAnimating ? {
                        x: [0, 100, 0],
                        scale: [1, 1.2, 1],
                      } : {}}
                      transition={{
                        duration: 2,
                        repeat: Infinity,
                        ease: "easeInOut"
                      }}
                    />
                  ) : (
                    <motion.div
                      className="absolute top-0 left-0 w-8 h-8 bg-netflix-red rounded-full"
                      animate={isAnimating ? {
                        x: [0, 100, 0],
                        scale: [1, 1.5, 1],
                        opacity: [1, 0.5, 1]
                      } : {}}
                      transition={{
                        duration: 2,
                        repeat: Infinity,
                        ease: "easeInOut"
                      }}
                    />
                  )}
                </div>
              </motion.div>
            ))}
          </div>
        </div>

        <div className="bg-gray-700 p-4 rounded-lg">
          <h4 className="text-xl font-semibold mb-2">Real-World Applications</h4>
          <ul className="list-disc list-inside space-y-2 text-gray-300">
            <li>Faster content recommendation algorithms</li>
            <li>More efficient video compression</li>
            <li>Enhanced security for streaming</li>
            <li>Optimized network routing</li>
          </ul>
        </div>
      </div>
    </div>
  );
};

export default QuantumVisualization; 