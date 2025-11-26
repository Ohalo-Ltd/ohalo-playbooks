"use client";

import { motion } from "framer-motion";
import React from "react";

export function BackgroundGradient() {
  return (
    <div className="absolute h-full inset-0 overflow-hidden -z-10 pointer-events-none">
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 1 }}
        className="absolute inset-0 bg-white dark:bg-zinc-950"
      >
        <div className="absolute bottom-[0%] left-[20%] w-[40vw] h-[40vw] bg-purple-200/15 dark:bg-purple-900/15 rounded-full blur-[100px] mix-blend-multiply dark:mix-blend-screen animate-blob" />
        <div className="absolute bottom-[0%] right-[20%] w-[35vw] h-[35vw] bg-indigo-200/15 dark:bg-indigo-900/15 rounded-full blur-[100px] mix-blend-multiply dark:mix-blend-screen animate-blob animation-delay-2000" />
        <div className="absolute bottom-[20%] left-[30%] w-[45vw] h-[45vw] bg-blue-200/15 dark:bg-blue-900/15 rounded-full blur-[100px] mix-blend-multiply dark:mix-blend-screen animate-blob animation-delay-4000" />
      </motion.div>
      <div className="absolute inset-0 bg-[url('/grid.svg')] opacity-[0.02] dark:opacity-[0.05]" />
    </div>
  );
}
