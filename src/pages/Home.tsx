import React from 'react';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import TechCard from '../components/TechCard';

const Home: React.FC = () => {
  const navigate = useNavigate();
  const technologies = [
    {
      title: 'Machine Learning',
      description: 'Powering personalized content recommendations and user experience optimization',
      icon: '🤖',
      color: 'bg-netflix-red'
    },
    {
      title: 'Deep Learning',
      description: 'Enhancing video quality and content understanding through neural networks',
      icon: '🧠',
      color: 'bg-blue-500'
    },
    {
      title: 'Quantum Computing',
      description: 'Exploring next-generation algorithms for content delivery optimization',
      icon: '⚛️',
      color: 'bg-purple-500'
    }
  ];

  const handleDemoClick = (tab: string) => {
    navigate(`/demo?tab=${tab}`);
  };

  return (
    <div className="container mx-auto px-4 py-16">
      {/* Features Section */}
      <motion.section
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.5 }}
        className="mb-20"
      >
        <h2 className="text-4xl font-bold text-center mb-12">
          Cutting-Edge Technologies
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {technologies.map((tech, index) => (
            <TechCard key={tech.title} {...tech} />
          ))}
        </div>
      </motion.section>

      {/* Interactive Demo Section */}
      <motion.section
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.8 }}
        className="bg-gray-800 rounded-2xl p-8 mb-20"
      >
        <h2 className="text-3xl font-bold mb-6">Interactive Demo</h2>
        <p className="text-gray-300 mb-8">
          Experience how Netflix's AI technologies work in real-time. Try our interactive demos to see
          machine learning, deep learning, and quantum computing in action.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="bg-gray-700 rounded-xl p-6"
          >
            <h3 className="text-xl font-bold mb-4">Recommendation System</h3>
            <p className="text-gray-300 mb-4">
              See how our AI analyzes your preferences to suggest the perfect content.
            </p>
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              className="bg-netflix-red text-white px-6 py-2 rounded-lg"
              onClick={() => handleDemoClick('tensorflow')}
            >
              Try Demo
            </motion.button>
          </motion.div>
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="bg-gray-700 rounded-xl p-6"
          >
            <h3 className="text-xl font-bold mb-4">Content Optimization</h3>
            <p className="text-gray-300 mb-4">
              Watch how our AI adapts video quality in real-time based on network conditions.
            </p>
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              className="bg-netflix-red text-white px-6 py-2 rounded-lg"
              onClick={() => handleDemoClick('deepLearning')}
            >
              Try Demo
            </motion.button>
          </motion.div>
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="bg-gray-700 rounded-xl p-6"
          >
            <h3 className="text-xl font-bold mb-4">Quantum AI</h3>
            <p className="text-gray-300 mb-4">
              Explore how quantum computing enhances our AI capabilities for better recommendations.
            </p>
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              className="bg-netflix-red text-white px-6 py-2 rounded-lg"
              onClick={() => handleDemoClick('quantumAI')}
            >
              Try Demo
            </motion.button>
          </motion.div>
        </div>
      </motion.section>

      {/* Call to Action */}
      <motion.section
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 1 }}
        className="text-center"
      >
        <h2 className="text-3xl font-bold mb-6">Ready to Explore More?</h2>
        <p className="text-gray-300 mb-8 max-w-2xl mx-auto">
          Dive deeper into each technology and discover how Netflix is shaping the future of streaming.
        </p>
        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          className="bg-netflix-red text-white px-8 py-3 rounded-lg text-lg font-semibold"
          onClick={() => handleDemoClick('deepLearning')}
        >
          Get Started
        </motion.button>
      </motion.section>
    </div>
  );
};

export default Home; 