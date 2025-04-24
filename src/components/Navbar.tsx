import React from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';

const Navbar: React.FC = () => {
  return (
    <nav className="bg-netflix-black p-4">
      <div className="container mx-auto flex justify-between items-center">
        <Link to="/" className="text-netflix-red text-2xl font-bold">
          Netflix AI
        </Link>
        <div className="space-x-6">
          <NavLink to="/recommendation-system">Recommendation System</NavLink>
          <NavLink to="/content-optimization">Content Optimization</NavLink>
          <NavLink to="/quantum-ai">Quantum AI</NavLink>
        </div>
      </div>
    </nav>
  );
};

const NavLink: React.FC<{ to: string; children: React.ReactNode }> = ({ to, children }) => {
  return (
    <Link to={to} className="text-gray-300 hover:text-white transition-colors">
      <motion.span
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
      >
        {children}
      </motion.span>
    </Link>
  );
};

export default Navbar; 