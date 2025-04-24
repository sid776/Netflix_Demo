import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import Home from './pages/Home';
import RecommendationSystem from './pages/RecommendationSystem';
import ContentOptimization from './pages/ContentOptimization';
import QuantumAI from './pages/QuantumAI';
import HeroSection from './components/HeroSection';
import Demo from './pages/Demo';

const App: React.FC = () => {
  return (
    <Router>
      <div className="min-h-screen bg-gray-900 text-white">
        <Navbar />
        <Routes>
          <Route path="/" element={
            <>
              <HeroSection />
              <Home />
            </>
          } />
          <Route path="/recommendation" element={<RecommendationSystem />} />
          <Route path="/optimization" element={<ContentOptimization />} />
          <Route path="/quantum" element={<QuantumAI />} />
          <Route path="/demo" element={<Demo />} />
        </Routes>
      </div>
    </Router>
  );
};

export default App; 