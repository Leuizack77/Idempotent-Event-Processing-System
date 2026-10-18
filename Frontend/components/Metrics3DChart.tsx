'use client';

import React, { useEffect, useRef } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { BarChart3, Rotate3D, Sparkles } from 'lucide-react';
import { SystemMetrics } from '@/lib/types';

interface Metrics3DChartProps {
  metrics: SystemMetrics;
}

export const Metrics3DChart: React.FC<Metrics3DChartProps> = ({ metrics }) => {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = mountRef.current;
    if (!container) return;

    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x020617, 0.04);

    const width = container.clientWidth || 450;
    const height = container.clientHeight || 300;

    const camera = new THREE.PerspectiveCamera(40, width / height, 0.1, 50);
    camera.position.set(4, 5, 8);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.shadowMap.enabled = true;
    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controls.autoRotate = true;
    controls.autoRotateSpeed = 1.2;
    controls.maxPolarAngle = Math.PI / 2.1; // prevent camera from going below floor

    // Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.8);
    scene.add(ambientLight);

    const directionalLight = new THREE.DirectionalLight(0x6366f1, 2.5);
    directionalLight.position.set(5, 10, 5);
    scene.add(directionalLight);

    // Floor Grid
    const grid = new THREE.GridHelper(12, 12, 0x334155, 0x0f172a);
    grid.position.y = 0;
    scene.add(grid);

    // 3D Bars Data Setup
    const barData = [
      { label: 'Submitted', val: Math.max(1, metrics.totalSubmitted), color: 0x38bdf8, emissive: 0x0284c7 },
      { label: 'Committed', val: Math.max(1, metrics.totalProcessed), color: 0x10b981, emissive: 0x059669 },
      { label: 'Redis Hits', val: Math.max(0.5, metrics.redisCacheHits), color: 0x34d399, emissive: 0x10b981 },
      { label: 'PG Dedup', val: Math.max(0.5, metrics.postgresConstraintHits), color: 0x818cf8, emissive: 0x6366f1 },
      { label: 'DLQ Error', val: Math.max(0.2, metrics.dlqTotal), color: 0xf43f5e, emissive: 0xe11d48 },
    ];

    const maxVal = Math.max(...barData.map(b => b.val), 5);
    const bars: THREE.Mesh[] = [];

    barData.forEach((b, i) => {
      const normalizedHeight = Math.max(0.3, (b.val / maxVal) * 3.5);
      const geo = new THREE.BoxGeometry(0.7, normalizedHeight, 0.7);
      const mat = new THREE.MeshStandardMaterial({
        color: b.color,
        emissive: b.emissive,
        emissiveIntensity: 0.6,
        roughness: 0.2,
        metalness: 0.8,
      });

      const mesh = new THREE.Mesh(geo, mat);
      // Position evenly along X axis
      mesh.position.set((i - 2) * 1.3, normalizedHeight / 2, 0);
      scene.add(mesh);
      bars.push(mesh);

      // Top glowing cap
      const capGeo = new THREE.BoxGeometry(0.72, 0.08, 0.72);
      const capMat = new THREE.MeshBasicMaterial({ color: 0xffffff });
      const cap = new THREE.Mesh(capGeo, capMat);
      cap.position.set(0, normalizedHeight / 2, 0);
      mesh.add(cap);
    });

    let animId: number;
    const animate = () => {
      animId = requestAnimationFrame(animate);
      controls.update();
      renderer.render(scene, camera);
    };
    animate();

    const handleResize = () => {
      if (!container) return;
      const w = container.clientWidth;
      const h = container.clientHeight;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h);
    };

    window.addEventListener('resize', handleResize);

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener('resize', handleResize);
      renderer.dispose();
    };
  }, [metrics]);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-sm flex flex-col justify-between">
      <div className="flex items-center justify-between pb-3 border-b border-slate-800">
        <h3 className="text-xs sm:text-sm font-bold text-white flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-emerald-400" />
          <span>3D Live Telemetry Bar Visualizer</span>
        </h3>
        <span className="text-[10px] sm:text-[11px] font-mono text-slate-400 flex items-center gap-1">
          <Rotate3D className="w-3.5 h-3.5 text-indigo-400" /> 3D Orbit
        </span>
      </div>

      {/* 3D Canvas Mount */}
      <div className="relative w-full h-[200px] sm:h-[220px] rounded-xl bg-slate-950/80 my-3 overflow-hidden border border-slate-800/60">
        <div ref={mountRef} className="w-full h-full cursor-grab active:cursor-grabbing touch-none" />
      </div>

      {/* Legend */}
      <div className="grid grid-cols-5 gap-1 text-center font-mono text-[10px]">
        <div className="text-cyan-400 font-semibold truncate">Submitted</div>
        <div className="text-emerald-400 font-semibold truncate">Committed</div>
        <div className="text-teal-400 font-semibold truncate">Redis</div>
        <div className="text-indigo-400 font-semibold truncate">Postgres</div>
        <div className="text-rose-400 font-semibold truncate">DLQ</div>
      </div>
    </div>
  );
};
