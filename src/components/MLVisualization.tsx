import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

interface DataItem {
  label: string;
  value: number;
}

interface Example {
  title: string;
  explanation: string;
  data: DataItem[];
}

interface Examples {
  [key: string]: Example;
}

const MLVisualization: React.FC = () => {
  const [selectedExample, setSelectedExample] = useState<string>('recommendation');
  const [isAnimating, setIsAnimating] = useState(false);

  const examples: Examples = {
    recommendation: {
      title: 'Content Recommendation',
      explanation: "Think of Netflix as a smart friend who knows your taste in movies. It learns from what you watch, like, and skip to suggest new shows you might enjoy. Just like how a friend might say \"If you liked this movie, you'll probably enjoy this one too!\"",
      data: [
        { label: 'Action Movies', value: 75 },
        { label: 'Comedies', value: 60 },
        { label: 'Dramas', value: 45 },
        { label: 'Documentaries', value: 30 },
      ]
    },
    optimization: {
      title: 'Video Quality Optimization',
      explanation: 'Imagine Netflix as a smart traffic controller for your internet. It constantly checks how fast your internet is and adjusts the video quality to ensure smooth playback. If your internet is slow, it might show a slightly lower quality video to prevent buffering.',
      data: [
        { label: '4K Quality', value: 20 },
        { label: 'HD Quality', value: 50 },
        { label: 'SD Quality', value: 30 },
      ]
    }
  };

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1
      }
    }
  };

  const itemVariants = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: {
        type: "spring",
        stiffness: 100
      }
    }
  };

  return (
    <div className="p-6 bg-gray-800 rounded-lg">
      <h3 className="text-2xl font-bold mb-4">Machine Learning in Action</h3>
      
      <div className="mb-6">
        <div className="flex space-x-4 mb-4">
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            className={`px-4 py-2 rounded ${
              selectedExample === 'recommendation'
                ? 'bg-netflix-red text-white'
                : 'bg-gray-700 text-gray-300'
            }`}
            onClick={() => setSelectedExample('recommendation')}
          >
            Recommendation System
          </motion.button>
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            className={`px-4 py-2 rounded ${
              selectedExample === 'optimization'
                ? 'bg-netflix-red text-white'
                : 'bg-gray-700 text-gray-300'
            }`}
            onClick={() => setSelectedExample('optimization')}
          >
            Quality Optimization
          </motion.button>
        </div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="bg-gray-700 p-4 rounded-lg mb-4"
        >
          <h4 className="text-xl font-semibold mb-2">{examples[selectedExample].title}</h4>
          <p className="text-gray-300">{examples[selectedExample].explanation}</p>
        </motion.div>

        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          className="bg-netflix-red text-white px-4 py-2 rounded mb-4"
          onClick={() => setIsAnimating(!isAnimating)}
        >
          {isAnimating ? 'Stop Animation' : 'Start Animation'}
        </motion.button>

        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="grid grid-cols-1 md:grid-cols-2 gap-4"
        >
          <AnimatePresence>
            {examples[selectedExample].data.map((item: DataItem, index: number) => (
              <motion.div
                key={item.label}
                variants={itemVariants}
                className="bg-gray-700 p-4 rounded-lg"
              >
                <div className="flex justify-between mb-2">
                  <span>{item.label}</span>
                  <motion.span
                    animate={isAnimating ? {
                      scale: [1, 1.2, 1],
                      color: ['#fff', '#E50914', '#fff']
                    } : {}}
                    transition={{
                      duration: 2,
                      repeat: Infinity,
                      delay: index * 0.2
                    }}
                  >
                    {item.value}%
                  </motion.span>
                </div>
                <div className="w-full bg-gray-600 rounded-full h-2.5">
                  <motion.div
                    className="bg-netflix-red h-2.5 rounded-full"
                    initial={{ width: 0 }}
                    animate={isAnimating ? {
                      width: [`${item.value}%`, `${item.value + 10}%`, `${item.value}%`],
                      scale: [1, 1.1, 1],
                      opacity: [1, 0.8, 1]
                    } : { width: `${item.value}%` }}
                    transition={{
                      duration: 2,
                      repeat: Infinity,
                      delay: index * 0.2
                    }}
                  />
                </div>
              </motion.div>
            ))}
          </AnimatePresence>
        </motion.div>
      </div>
    </div>
  );
};

export default MLVisualization; 