'use client';

import React, { useEffect, useRef } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { BarChart3, Rotate3D, Activity } from 'lucide-react';
import { SystemMetrics } from '@/lib/types';

interface Metrics3DChartProps {
  metrics: SystemMetrics;
}

export const Metrics3DChart: React.FC<Metrics3DChartProps> = ({ metrics }) => {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = mountRef.current;
    if (!container) return;

    // 1. Scene & Linear Palette Fog
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x09090b, 0.04);

    const width = container.clientWidth || 450;
    const height = container.clientHeight || 280;

    const camera = new THREE.PerspectiveCamera(38, width / height, 0.1, 50);
    camera.position.set(4.5, 5.5, 7.5);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.2;
    renderer.shadowMap.enabled = true;
    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controls.autoRotate = true;
    controls.autoRotateSpeed = 0.8;
    controls.maxPolarAngle = Math.PI / 2.15; // prevent going below floor
    controls.minDistance = 4;
    controls.maxDistance = 16;

    // 2. Realistic Studio Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.8);
    scene.add(ambientLight);

    const keyLight = new THREE.DirectionalLight(0xffffff, 2.0);
    keyLight.position.set(6, 10, 6);
    scene.add(keyLight);

    const rimLight = new THREE.DirectionalLight(0x6366f1, 1.2);
    rimLight.position.set(-6, -2, -4);
    scene.add(rimLight);

    // 3. Dark Architectural Floor Platform
    const floorGeo = new THREE.PlaneGeometry(16, 16);
    const floorMat = new THREE.MeshStandardMaterial({
      color: 0x09090b,
      roughness: 0.9,
      metalness: 0.2,
    });
    const floor = new THREE.Mesh(floorGeo, floorMat);
    floor.rotation.x = -Math.PI / 2;
    floor.position.y = 0;
    scene.add(floor);

    const grid = new THREE.GridHelper(16, 16, 0x27272a, 0x18181b);
    grid.position.y = 0.005;
    scene.add(grid);

    // 4. Materials
    const titaniumMat = new THREE.MeshStandardMaterial({
      color: 0x18181b,
      metalness: 0.9,
      roughness: 0.2,
    });

    const brushedCapMat = new THREE.MeshStandardMaterial({
      color: 0xa1a1aa,
      metalness: 0.95,
      roughness: 0.15,
    });

    // 5. Data Metrics Columns Setup
    const barData = [
      { label: 'Submitted', val: Math.max(1, metrics.totalSubmitted), color: 0x38bdf8, emissive: 0x0284c7 },
      { label: 'Committed', val: Math.max(1, metrics.totalProcessed), color: 0x10b981, emissive: 0x059669 },
      { label: 'Redis Hits', val: Math.max(0.5, metrics.redisCacheHits), color: 0x34d399, emissive: 0x10b981 },
      { label: 'PG Dedup', val: Math.max(0.5, metrics.postgresConstraintHits), color: 0x8b5cf6, emissive: 0x6d28d9 },
      { label: 'DLQ Error', val: Math.max(0.2, metrics.dlqTotal), color: 0xf43f5e, emissive: 0xbe123c },
    ];

    const maxVal = Math.max(...barData.map(b => b.val), 5);
    const columnGroups: THREE.Group[] = [];

    barData.forEach((b, i) => {
      const colGroup = new THREE.Group();
      const posX = (i - 2) * 1.35;
      colGroup.position.set(posX, 0, 0);

      const normalizedHeight = Math.max(0.4, (b.val / maxVal) * 3.4);

      // A. Titanium Pedestal Base
      const baseGeo = new THREE.CylinderGeometry(0.55, 0.62, 0.15, 28);
      const baseMesh = new THREE.Mesh(baseGeo, titaniumMat);
      baseMesh.position.y = 0.075;
      colGroup.add(baseMesh);

      // Base Glowing Inset Ring
      const ringGeo = new THREE.TorusGeometry(0.56, 0.02, 8, 28);
      const ringMat = new THREE.MeshBasicMaterial({ color: b.color, transparent: true, opacity: 0.8 });
      const ring = new THREE.Mesh(ringGeo, ringMat);
      ring.rotation.x = Math.PI / 2;
      ring.position.y = 0.12;
      colGroup.add(ring);

      // B. Inner Glowing Energy Core
      const coreGeo = new THREE.CylinderGeometry(0.25, 0.25, normalizedHeight, 24);
      const coreMat = new THREE.MeshStandardMaterial({
        color: b.color,
        emissive: b.emissive,
        emissiveIntensity: 1.2,
        roughness: 0.2,
        metalness: 0.5,
      });
      const core = new THREE.Mesh(coreGeo, coreMat);
      core.position.y = 0.15 + normalizedHeight / 2;
      colGroup.add(core);

      // C. Outer Translucent Precision Glass Shroud
      const glassGeo = new THREE.CylinderGeometry(0.46, 0.46, normalizedHeight, 28);
      const glassMat = new THREE.MeshStandardMaterial({
        color: 0x18181b,
        roughness: 0.1,
        metalness: 0.1,
        transparent: true,
        opacity: 0.45,
      });
      const glass = new THREE.Mesh(glassGeo, glassMat);
      glass.position.y = 0.15 + normalizedHeight / 2;
      colGroup.add(glass);

      // D. Top Brushed Aluminum Cap & Sensor Beacon
      const capGeo = new THREE.CylinderGeometry(0.48, 0.48, 0.09, 28);
      const cap = new THREE.Mesh(capGeo, brushedCapMat);
      cap.position.y = 0.15 + normalizedHeight + 0.045;
      colGroup.add(cap);

      const beaconGeo = new THREE.SphereGeometry(0.08, 12, 12);
      const beaconMat = new THREE.MeshBasicMaterial({ color: b.color });
      const beacon = new THREE.Mesh(beaconGeo, beaconMat);
      beacon.position.y = 0.15 + normalizedHeight + 0.14;
      colGroup.add(beacon);

      scene.add(colGroup);
      columnGroups.push(colGroup);
    });

    let animId: number;
    let clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      controls.update();

      // Subtle breathing pulse on column beacons
      columnGroups.forEach((cg, idx) => {
        const beacon = cg.children[cg.children.length - 1];
        if (beacon) {
          beacon.position.y = (beacon.position.y) + Math.sin(elapsed * 2.5 + idx) * 0.001;
        }
      });

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
    <div className="bg-zinc-900/40 border border-zinc-800/80 rounded-2xl p-4 sm:p-5 shadow-xl flex flex-col justify-between">
      {/* Header */}
      <div className="flex items-center justify-between pb-3 border-b border-zinc-800/80">
        <h3 className="text-xs sm:text-sm font-semibold text-white flex items-center gap-2">
          <Activity className="w-4 h-4 text-zinc-400" />
          <span>3D Telemetry Metrics Matrix</span>
        </h3>
        <span className="text-[10px] sm:text-[11px] font-mono text-zinc-400 flex items-center gap-1 bg-zinc-800 px-2 py-0.5 rounded-md border border-zinc-700/60">
          <Rotate3D className="w-3 h-3 text-zinc-300" /> 3D Orbit Matrix
        </span>
      </div>

      {/* 3D Canvas Mount */}
      <div className="relative w-full h-[200px] sm:h-[220px] rounded-xl bg-zinc-950 my-3.5 overflow-hidden border border-zinc-800 shadow-inner">
        <div ref={mountRef} className="w-full h-full cursor-grab active:cursor-grabbing touch-none" />
      </div>

      {/* High-Density Data Telemetry Legend */}
      <div className="grid grid-cols-5 gap-1.5 text-center font-mono text-[10px] bg-zinc-950/80 p-2 rounded-xl border border-zinc-800/80">
        <div className="truncate">
          <span className="text-sky-400 font-bold block">{metrics.totalSubmitted}</span>
          <span className="text-zinc-500">Ingested</span>
        </div>
        <div className="truncate">
          <span className="text-emerald-400 font-bold block">{metrics.totalProcessed}</span>
          <span className="text-zinc-500">Committed</span>
        </div>
        <div className="truncate">
          <span className="text-teal-400 font-bold block">{metrics.redisCacheHits}</span>
          <span className="text-zinc-500">Redis L1</span>
        </div>
        <div className="truncate">
          <span className="text-purple-400 font-bold block">{metrics.postgresConstraintHits}</span>
          <span className="text-zinc-500">Postgres</span>
        </div>
        <div className="truncate">
          <span className="text-rose-400 font-bold block">{metrics.dlqTotal}</span>
          <span className="text-zinc-500">DLQ</span>
        </div>
      </div>
    </div>
  );
};
