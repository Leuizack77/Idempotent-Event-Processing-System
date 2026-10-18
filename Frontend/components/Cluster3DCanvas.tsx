'use client';

import React, { useEffect, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { 
  Box, 
  Maximize2, 
  RotateCcw, 
  Sparkles, 
  Play, 
  Pause, 
  Info, 
  Zap, 
  Layers, 
  Database, 
  ShieldCheck,
  AlertOctagon
} from 'lucide-react';
import { SystemMetrics } from '@/lib/types';

interface Cluster3DCanvasProps {
  metrics: SystemMetrics;
  onTriggerBurst?: () => void;
}

interface NodeData {
  id: string;
  name: string;
  category: string;
  role: string;
  position: [number, number, number];
  color: number;
  emissive: number;
  stats: string;
}

const NODES: NodeData[] = [
  {
    id: 'gateway',
    name: 'REST Ingestion Gateway',
    category: 'Spring Boot 3.3',
    role: 'Synchronous validation, atomic SUBMITTED state insertion, MDC trace assignment',
    position: [-6, 0.5, 0],
    color: 0x38bdf8,
    emissive: 0x0284c7,
    stats: 'Port: 8080 • Non-blocking HTTP 202',
  },
  {
    id: 'kafka',
    name: 'Apache Kafka Cluster',
    category: 'KRaft Distributed Broker',
    role: 'Keyed partition routing (key=eventId), at-least-once transport, consumer group lag isolation',
    position: [-1.8, 2.5, 1],
    color: 0xfbbf24,
    emissive: 0xd97706,
    stats: 'Topic: "events" • 3 Partitions',
  },
  {
    id: 'redis',
    name: 'Redis L1 Fast-Path Cache',
    category: 'In-Memory Key-Value',
    role: 'Non-authoritative read-through caching. Sub-millisecond short-circuit (<3ms) for COMPLETED events',
    position: [2.5, 2.5, -0.5],
    color: 0x34d399,
    emissive: 0x059669,
    stats: 'TTL: 24h • Fail-Open Enabled',
  },
  {
    id: 'postgres',
    name: 'PostgreSQL 16 Master',
    category: 'Authoritative Source of Truth',
    role: 'ACID unique constraint (uq_processed_events_event_id), UUID fencing tokens, lease reclaims',
    position: [3.5, -1.8, 1],
    color: 0x818cf8,
    emissive: 0x4f46e5,
    stats: 'ACID • REQUIRES_NEW Isolated Txn',
  },
  {
    id: 'dlq',
    name: 'Dead Letter Queue (DLQ)',
    category: 'Kafka Quarantine Topic',
    role: 'Poison pill isolation after 3 exponential backoff attempts. Prevents partition blockage',
    position: [-1.5, -2.5, -1],
    color: 0xf43f5e,
    emissive: 0xe11d48,
    stats: 'Topic: "events.dlq" • Poison Pill Safe',
  },
];

export const Cluster3DCanvas: React.FC<Cluster3DCanvasProps> = ({ metrics, onTriggerBurst }) => {
  const mountRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeData | null>(NODES[1]); // default Kafka
  const [autoRotate, setAutoRotate] = useState<boolean>(true);
  const [pulseCount, setPulseCount] = useState<number>(0);

  // References to communicate with Three.js render loop
  const triggerPulseRef = useRef<(() => void) | null>(null);

  useEffect(() => {
    const container = mountRef.current;
    if (!container) return;

    // 1. Scene, Camera, Renderer
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x020617, 0.035);

    const width = container.clientWidth || 800;
    const height = container.clientHeight || 520;

    const isMobile = width < 640;
    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.set(0, isMobile ? 3 : 4, isMobile ? 18 : 14);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.shadowMap.enabled = true;
    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    // 2. Controls
    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controls.maxDistance = 28;
    controls.minDistance = 4;
    controls.autoRotate = autoRotate;
    controls.autoRotateSpeed = 0.9;

    // 3. Lighting
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.9);
    scene.add(ambientLight);

    const pointLight = new THREE.PointLight(0x6366f1, 3, 30);
    pointLight.position.set(0, 8, 5);
    scene.add(pointLight);

    const cyanLight = new THREE.PointLight(0x38bdf8, 2, 20);
    cyanLight.position.set(-6, 2, 2);
    scene.add(cyanLight);

    const emeraldLight = new THREE.PointLight(0x10b981, 2, 20);
    emeraldLight.position.set(4, 3, 2);
    scene.add(emeraldLight);

    // 4. Background Starfield / Particle Dust
    const particleGeo = new THREE.BufferGeometry();
    const particleCount = 450;
    const posArray = new Float32Array(particleCount * 3);
    for (let i = 0; i < particleCount * 3; i++) {
      posArray[i] = (Math.random() - 0.5) * 35;
    }
    particleGeo.setAttribute('position', new THREE.BufferAttribute(posArray, 3));
    const particleMat = new THREE.PointsMaterial({
      size: 0.05,
      color: 0x94a3b8,
      transparent: true,
      opacity: 0.4,
    });
    const starField = new THREE.Points(particleGeo, particleMat);
    scene.add(starField);

    // 5. Grid helper in 3D floor
    const grid = new THREE.GridHelper(30, 30, 0x1e293b, 0x0f172a);
    grid.position.y = -3.8;
    scene.add(grid);

    // 6. Build 3D Nodes
    const nodeMeshes: THREE.Group[] = [];
    const interactiveMeshes: THREE.Mesh[] = [];

    NODES.forEach((node) => {
      const group = new THREE.Group();
      group.position.set(...node.position);

      let mainMesh: THREE.Mesh;

      if (node.id === 'gateway') {
        const geo = new THREE.IcosahedronGeometry(0.9, 0);
        const mat = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: node.emissive,
          emissiveIntensity: 0.6,
          roughness: 0.2,
          metalness: 0.8,
        });
        mainMesh = new THREE.Mesh(geo, mat);

        // Outer rotating wireframe
        const wireGeo = new THREE.IcosahedronGeometry(1.2, 1);
        const wireMat = new THREE.MeshBasicMaterial({ color: 0x38bdf8, wireframe: true, transparent: true, opacity: 0.25 });
        const wireMesh = new THREE.Mesh(wireGeo, wireMat);
        group.add(wireMesh);
      } else if (node.id === 'kafka') {
        const geo = new THREE.CylinderGeometry(0.85, 0.85, 1.2, 16);
        const mat = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: node.emissive,
          emissiveIntensity: 0.7,
          roughness: 0.3,
          metalness: 0.7,
        });
        mainMesh = new THREE.Mesh(geo, mat);

        // 3 Partition rings
        for (let r = 0; r < 3; r++) {
          const torusGeo = new THREE.TorusGeometry(1.2 + r * 0.22, 0.02, 8, 32);
          const torusMat = new THREE.MeshBasicMaterial({ color: 0xfbbf24, transparent: true, opacity: 0.6 });
          const torus = new THREE.Mesh(torusGeo, torusMat);
          torus.rotation.x = Math.PI / 2;
          torus.position.y = (r - 1) * 0.3;
          group.add(torus);
        }
      } else if (node.id === 'redis') {
        const geo = new THREE.CylinderGeometry(0.9, 0.9, 0.9, 6);
        const mat = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: node.emissive,
          emissiveIntensity: 0.75,
          roughness: 0.2,
          metalness: 0.6,
        });
        mainMesh = new THREE.Mesh(geo, mat);

        // Fast-path halo ring
        const haloGeo = new THREE.RingGeometry(1.1, 1.3, 16);
        const haloMat = new THREE.MeshBasicMaterial({ color: 0x34d399, side: THREE.DoubleSide, transparent: true, opacity: 0.4 });
        const halo = new THREE.Mesh(haloGeo, haloMat);
        halo.rotation.x = Math.PI / 2;
        group.add(halo);
      } else if (node.id === 'postgres') {
        const geo = new THREE.CylinderGeometry(1.0, 1.0, 1.3, 20);
        const mat = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: node.emissive,
          emissiveIntensity: 0.6,
          roughness: 0.25,
          metalness: 0.9,
        });
        mainMesh = new THREE.Mesh(geo, mat);

        // Fencing token aura ring
        const tokenRingGeo = new THREE.TorusGeometry(1.4, 0.035, 12, 48);
        const tokenRingMat = new THREE.MeshStandardMaterial({ color: 0x818cf8, emissive: 0x6366f1, emissiveIntensity: 1 });
        const tokenRing = new THREE.Mesh(tokenRingGeo, tokenRingMat);
        tokenRing.rotation.x = Math.PI / 2;
        group.add(tokenRing);
      } else {
        // DLQ
        const geo = new THREE.OctahedronGeometry(0.85, 0);
        const mat = new THREE.MeshStandardMaterial({
          color: node.color,
          emissive: node.emissive,
          emissiveIntensity: 0.8,
          roughness: 0.2,
          metalness: 0.7,
        });
        mainMesh = new THREE.Mesh(geo, mat);
      }

      mainMesh.userData = { nodeData: node };
      interactiveMeshes.push(mainMesh);
      group.add(mainMesh);
      scene.add(group);
      nodeMeshes.push(group);
    });

    // 7. 3D Curved Splines Connecting the Nodes
    const connectionPairs = [
      { from: NODES[0].position, to: NODES[1].position, color: 0x38bdf8 }, // Ingest -> Kafka
      { from: NODES[1].position, to: NODES[2].position, color: 0xfbbf24 }, // Kafka -> Redis
      { from: NODES[2].position, to: NODES[3].position, color: 0x34d399 }, // Redis -> Postgres
      { from: NODES[1].position, to: NODES[4].position, color: 0xf43f5e }, // Kafka -> DLQ
    ];

    const curves: THREE.QuadraticBezierCurve3[] = [];

    connectionPairs.forEach(pair => {
      const vFrom = new THREE.Vector3(...pair.from);
      const vTo = new THREE.Vector3(...pair.to);
      const mid = new THREE.Vector3().addVectors(vFrom, vTo).multiplyScalar(0.5);
      mid.y += 1.2; // elevated arch curve

      const curve = new THREE.QuadraticBezierCurve3(vFrom, mid, vTo);
      curves.push(curve);

      // Tube geometry for glowing path
      const tubeGeo = new THREE.TubeGeometry(curve, 32, 0.03, 8, false);
      const tubeMat = new THREE.MeshBasicMaterial({ color: pair.color, transparent: true, opacity: 0.35 });
      const tubeMesh = new THREE.Mesh(tubeGeo, tubeMat);
      scene.add(tubeMesh);
    });

    // 8. Glowing Animated Packets traversing the splines
    const packets: { mesh: THREE.Mesh; curveIndex: number; progress: number; speed: number }[] = [];
    const packetGeo = new THREE.SphereGeometry(0.12, 12, 12);

    for (let i = 0; i < 8; i++) {
      const cIdx = i % curves.length;
      const color = connectionPairs[cIdx].color;
      const packetMat = new THREE.MeshBasicMaterial({ color });
      const packetMesh = new THREE.Mesh(packetGeo, packetMat);
      scene.add(packetMesh);

      packets.push({
        mesh: packetMesh,
        curveIndex: cIdx,
        progress: Math.random(),
        speed: 0.006 + Math.random() * 0.004,
      });
    }

    // Burst trigger function to rapidly animate packets
    triggerPulseRef.current = () => {
      packets.forEach(p => {
        p.speed = 0.025; // speed burst
        setTimeout(() => { p.speed = 0.008; }, 2000);
      });
    };

    // 9. Raycasting for Mouse Hover & Selection
    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();

    const onPointerDown = (event: MouseEvent) => {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

      raycaster.setFromCamera(mouse, camera);
      const intersects = raycaster.intersectObjects(interactiveMeshes);

      if (intersects.length > 0) {
        const hitData = intersects[0].object.userData.nodeData as NodeData;
        setSelectedNode(hitData);
      }
    };

    renderer.domElement.addEventListener('pointerdown', onPointerDown);

    // 10. Animation Loop
    let animationFrameId: number;
    let clock = new THREE.Clock();

    const animate = () => {
      animationFrameId = requestAnimationFrame(animate);
      const elapsedTime = clock.getElapsedTime();

      // Update Controls
      controls.autoRotate = autoRotate;
      controls.update();

      // Gently rotate individual nodes & rings
      nodeMeshes.forEach((group, idx) => {
        group.rotation.y = elapsedTime * (0.3 + idx * 0.1);
        group.position.y += Math.sin(elapsedTime * 2 + idx) * 0.0015;
      });

      // Animate starfield
      starField.rotation.y = elapsedTime * 0.02;

      // Animate packet positions along curves
      packets.forEach(p => {
        p.progress += p.speed;
        if (p.progress > 1) p.progress = 0;
        const pt = curves[p.curveIndex].getPoint(p.progress);
        p.mesh.position.copy(pt);
      });

      renderer.render(scene, camera);
    };

    animate();

    // 11. Responsive Resize
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
      cancelAnimationFrame(animationFrameId);
      window.removeEventListener('resize', handleResize);
      renderer.domElement.removeEventListener('pointerdown', onPointerDown);
      renderer.dispose();
    };
  }, [autoRotate]);

  const handlePulse = () => {
    setPulseCount(prev => prev + 1);
    if (triggerPulseRef.current) {
      triggerPulseRef.current();
    }
    if (onTriggerBurst) {
      onTriggerBurst();
    }
  };

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl sm:rounded-3xl p-4 sm:p-6 shadow-2xl relative overflow-hidden">
      {/* 3D Header HUD */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 border-b border-slate-800 gap-3 relative z-10">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Box className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 animate-spin-slow shrink-0" />
              <span>3D Topology & Event Stream Canvas</span>
            </h2>
            <span className="hidden xs:inline-flex px-2 py-0.5 rounded-full text-[10px] sm:text-xs font-semibold bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 font-mono">
              WebGL
            </span>
          </div>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
            Drag to rotate 360° • Pinch to zoom • Tap nodes to inspect
          </p>
        </div>

        {/* 3D Controls Button Group */}
        <div className="flex items-center gap-2 shrink-0">
          <button
            onClick={() => setAutoRotate(!autoRotate)}
            className={`px-2.5 sm:px-3 py-1.5 rounded-xl border text-[11px] sm:text-xs font-medium flex items-center gap-1.5 transition ${
              autoRotate 
                ? 'bg-indigo-600/20 border-indigo-500/40 text-indigo-300' 
                : 'bg-slate-950 border-slate-800 text-slate-400 hover:text-white'
            }`}
          >
            {autoRotate ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
            <span className="hidden xs:inline">{autoRotate ? 'Pause' : 'Rotate'}</span>
          </button>

          <button
            onClick={handlePulse}
            className="px-3 sm:px-3.5 py-1.5 rounded-xl bg-gradient-to-r from-amber-500 to-indigo-600 hover:from-amber-400 hover:to-indigo-500 text-white font-semibold text-[11px] sm:text-xs shadow-lg shadow-indigo-500/20 flex items-center gap-1.5 transition active:scale-95"
          >
            <Sparkles className="w-3.5 h-3.5" />
            <span>Burst Pulse</span>
          </button>
        </div>
      </div>

      {/* Main 3D Canvas Box */}
      <div className="relative w-full h-[320px] xs:h-[380px] sm:h-[450px] lg:h-[500px] rounded-xl sm:rounded-2xl overflow-hidden bg-slate-950 border border-slate-800/80 mt-4">
        {/* Three.js Container Mount */}
        <div ref={mountRef} className="w-full h-full cursor-grab active:cursor-grabbing touch-none" />

        {/* Floating 3D Legend & Node Stats Overlay */}
        {selectedNode && (
          <div className="absolute top-3 left-3 sm:top-4 sm:left-4 max-w-[240px] xs:max-w-[280px] sm:max-w-xs bg-slate-900/90 backdrop-blur-md border border-slate-700/80 rounded-xl sm:rounded-2xl p-3 sm:p-4 shadow-2xl animate-fadeIn font-sans z-20">
            <div className="flex items-center justify-between pb-1.5 mb-1.5 border-b border-slate-800">
              <span className="text-[9px] sm:text-[10px] font-mono font-bold text-indigo-400 uppercase tracking-wider">
                {selectedNode.category}
              </span>
              <span className="w-1.5 h-1.5 sm:w-2 sm:h-2 rounded-full bg-emerald-400 animate-ping" />
            </div>
            <h3 className="text-xs sm:text-sm font-extrabold text-white">{selectedNode.name}</h3>
            <p className="text-[11px] sm:text-xs text-slate-300 mt-1 leading-relaxed line-clamp-3 sm:line-clamp-none">
              {selectedNode.role}
            </p>
            <div className="mt-2.5 pt-1.5 border-t border-slate-800/80 text-[10px] sm:text-[11px] font-mono text-emerald-400 flex items-center gap-1">
              <Info className="w-3 h-3 shrink-0" />
              <span className="truncate">{selectedNode.stats}</span>
            </div>
          </div>
        )}

        {/* 3D Real-Time Telemetry Bar floating in bottom center */}
        <div className="absolute bottom-2.5 left-2.5 right-2.5 sm:bottom-4 sm:left-4 sm:right-4 bg-slate-900/90 backdrop-blur-md border border-slate-800 rounded-xl sm:rounded-2xl p-2.5 sm:p-3.5 flex items-center justify-between gap-2 z-20 text-[11px] sm:text-xs font-mono">
          <div className="flex items-center gap-3 sm:gap-6 overflow-x-auto no-scrollbar">
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-cyan-400" />
              <span className="text-slate-300">Gateway</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-amber-400" />
              <span className="text-slate-300">Kafka</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-emerald-400" />
              <span className="text-slate-300">Redis L1</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-indigo-400" />
              <span className="text-slate-300">PostgreSQL</span>
            </div>
          </div>

          <div className="text-slate-400 text-[10px] sm:text-[11px] shrink-0 font-sans">
            Pulses: <span className="text-amber-400 font-bold font-mono">{pulseCount}</span>
          </div>
        </div>
      </div>
    </div>
  );
};
