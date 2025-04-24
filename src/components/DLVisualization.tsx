import React, { useState } from 'react';
import { motion } from 'framer-motion';

const DLVisualization: React.FC = () => {
  const [step, setStep] = useState(0);

  const steps = [
    {
      title: 'Content Analysis',
      explanation: 'Think of Netflix\'s deep learning system as a super-smart movie critic. It watches and analyzes every frame of content, understanding things like the mood, action scenes, dialogue, and even the music. It\'s like having thousands of movie experts watching and categorizing content simultaneously.',
      visualization: (
        <div className="grid grid-cols-3 gap-4">
          {['🎬', '🎭', '🎵'].map((emoji, index) => (
            <motion.div
              key={index}
              className="bg-gray-700 p-4 rounded-lg text-center text-4xl"
              initial={{ scale: 0 }}
              animate={{ scale: 1 }}
              transition={{ delay: index * 0.2 }}
            >
              {emoji}
            </motion.div>
          ))}
        </div>
      )
    },
    {
      title: 'Pattern Recognition',
      explanation: 'The system learns to recognize patterns in how people watch content. For example, it might notice that people who enjoy sci-fi movies often also like certain types of documentaries. It\'s like having a friend who remembers all your preferences and can spot connections you might not even notice yourself.',
      visualization: (
        <div className="relative h-40">
          <motion.div
            className="absolute top-0 left-0 w-20 h-20 bg-netflix-red rounded-full"
            animate={{
              x: [0, 100, 0],
              y: [0, 50, 0],
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: "easeInOut"
            }}
          />
          <motion.div
            className="absolute top-20 left-20 w-20 h-20 bg-blue-500 rounded-full"
            animate={{
              x: [0, -100, 0],
              y: [0, -50, 0],
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: "easeInOut"
            }}
          />
        </div>
      )
    },
    {
      title: 'Personalization',
      explanation: 'Finally, the system combines all this knowledge to create a unique experience for each viewer. It\'s like having a personal TV guide that knows exactly what you want to watch, even before you do!',
      visualization: (
        <div className="flex justify-center space-x-4">
          {['👤', '🎯', '📺'].map((emoji, index) => (
            <motion.div
              key={index}
              className="bg-gray-700 p-4 rounded-lg text-4xl"
              animate={{
                y: [0, -10, 0],
              }}
              transition={{
                duration: 1,
                repeat: Infinity,
                delay: index * 0.2
              }}
            >
              {emoji}
            </motion.div>
          ))}
        </div>
      )
    }
  ];

  return (
    <div className="p-6 bg-gray-800 rounded-lg">
      <h3 className="text-2xl font-bold mb-4">Deep Learning Process</h3>
      
      <div className="mb-6">
        <div className="flex justify-between mb-4">
          {steps.map((_, index) => (
            <button
              key={index}
              className={`w-8 h-8 rounded-full ${
                step === index ? 'bg-netflix-red' : 'bg-gray-700'
              }`}
              onClick={() => setStep(index)}
            />
          ))}
        </div>

        <motion.div
          key={step}
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -20 }}
          className="bg-gray-700 p-4 rounded-lg mb-4"
        >
          <h4 className="text-xl font-semibold mb-2">{steps[step].title}</h4>
          <p className="text-gray-300 mb-4">{steps[step].explanation}</p>
          {steps[step].visualization}
        </motion.div>
      </div>
    </div>
  );
};

export default DLVisualization; 