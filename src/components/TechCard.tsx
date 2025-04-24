import React from 'react';
import { motion } from 'framer-motion';

interface TechCardProps {
  title: string;
  description: string;
  icon: string;
  color: string;
}

const TechCard: React.FC<TechCardProps> = ({ title, description, icon, color }) => {
  return (
    <motion.div
      className="bg-gray-800 rounded-xl p-6 h-full"
      whileHover={{ 
        scale: 1.02,
        transition: { duration: 0.2 }
      }}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
    >
      <div className={`w-12 h-12 rounded-lg ${color} flex items-center justify-center mb-4`}>
        <span className="text-2xl">{icon}</span>
      </div>
      <h3 className="text-xl font-bold mb-2">{title}</h3>
      <p className="text-gray-300">{description}</p>
    </motion.div>
  );
};

export default TechCard; 