'use client';

import React, { useEffect, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { 
  Box, 
  RotateCcw, 
  Sparkles, 
  Play, 
  Pause, 
  Info, 
  Zap, 
  Layers, 
  Database, 
  ShieldCheck,
  Server,
  Activity,
  HardDrive
} from 'lucide-react';
import { SystemMetrics } from '@/lib/types';
import { motion } from 'motion/react';

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
    position: [-5.8, 0.4, 0],
    color: 0x38bdf8,
    emissive: 0x0284c7,
    stats: 'Port: 8080 • Non-blocking HTTP 202',
  },
  {
    id: 'kafka',
    name: 'Apache Kafka Cluster',
    category: 'KRaft Distributed Broker',
    role: 'Keyed partition routing (key=eventId), at-least-once transport, consumer group lag isolation',
    position: [-1.8, 2.0, 1],
    color: 0xf59e0b,
    emissive: 0xd97706,
    stats: 'Topic: "events" • 3 Partitions (0, 1, 2)',
  },
  {
    id: 'redis',
    name: 'Redis L1 Fast-Path Cache',
    category: 'In-Memory Memory Core',
    role: 'Non-authoritative read-through caching. Sub-millisecond short-circuit (<3ms) for COMPLETED events',
    position: [2.8, 2.0, -0.4],
    color: 0x10b981,
    emissive: 0x059669,
    stats: 'TTL: 24h • Fail-Open Enabled',
  },
  {
    id: 'postgres',
    name: 'PostgreSQL 16 Master',
    category: 'Authoritative ACID Storage Array',
    role: 'ACID unique constraint (uq_processed_events_event_id), UUID fencing tokens, lease reclaims',
    position: [3.8, -1.5, 0.8],
    color: 0x8b5cf6,
    emissive: 0x6d28d9,
    stats: 'ACID • REQUIRES_NEW Isolated Txn',
  },
  {
    id: 'dlq',
    name: 'Dead Letter Queue (DLQ)',
    category: 'Kafka Quarantine Isolation Vault',
    role: 'Poison pill isolation after 3 exponential backoff attempts. Prevents partition blockage',
    position: [-1.6, -2.2, -0.8],
    color: 0xf43f5e,
    emissive: 0xbe123c,
    stats: 'Topic: "events.dlq" • Poison Pill Safe',
  },
];

export const Cluster3DCanvas: React.FC<Cluster3DCanvasProps> = ({ metrics, onTriggerBurst }) => {
  const mountRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeData | null>(NODES[1]); // default Kafka
  const [autoRotate, setAutoRotate] = useState<boolean>(true);
  const [pulseCount, setPulseCount] = useState<number>(0);

  const triggerPulseRef = useRef<(() => void) | null>(null);

  useEffect(() => {
    const container = mountRef.current;
    if (!container) return;

    // 1. Scene & Atmosphere (Matches Linear Obsidian)
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x09090b, 0.035);

    const width = container.clientWidth || 800;
    const height = container.clientHeight || 520;
    const isMobile = width < 640;

    const camera = new THREE.PerspectiveCamera(42, width / height, 0.1, 100);
    camera.position.set(0, isMobile ? 3.5 : 4.5, isMobile ? 18 : 14.5);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.2;
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;

    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    // 2. Controls
    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controls.maxDistance = 26;
    controls.minDistance = 5;
    controls.autoRotate = autoRotate;
    controls.autoRotateSpeed = 0.6;
    controls.maxPolarAngle = Math.PI / 2 + 0.1;

    // 3. Realistic Studio Lighting
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.7);
    scene.add(ambientLight);

    const keyLight = new THREE.DirectionalLight(0xffffff, 1.8);
    keyLight.position.set(8, 12, 10);
    scene.add(keyLight);

    const fillLight = new THREE.DirectionalLight(0x6366f1, 0.8);
    fillLight.position.set(-10, -4, -6);
    scene.add(fillLight);

    // 4. Datacenter Floor Grid Platform
    const floorGeo = new THREE.PlaneGeometry(36, 36);
    const floorMat = new THREE.MeshStandardMaterial({
      color: 0x09090b,
      roughness: 0.85,
      metalness: 0.3,
    });
    const floor = new THREE.Mesh(floorGeo, floorMat);
    floor.rotation.x = -Math.PI / 2;
    floor.position.y = -3.8;
    scene.add(floor);

    const grid = new THREE.GridHelper(36, 36, 0x27272a, 0x18181b);
    grid.position.y = -3.79;
    scene.add(grid);

    // 5. Materials Library (PBR High-Tech)
    const chassisMat = new THREE.MeshStandardMaterial({
      color: 0x18181b,
      metalness: 0.85,
      roughness: 0.25,
    });

    const brushedAluMat = new THREE.MeshStandardMaterial({
      color: 0x71717a,
      metalness: 0.95,
      roughness: 0.15,
    });

    const darkTrimMat = new THREE.MeshStandardMaterial({
      color: 0x09090b,
      metalness: 0.6,
      roughness: 0.5,
    });

    // 6. Build High-End Realistic Models
    const nodeMeshes: THREE.Group[] = [];
    const interactiveMeshes: THREE.Mesh[] = [];

    // Helper: Add pedestal under each node
    const createPedestal = (color: number) => {
      const pGroup = new THREE.Group();
      const baseGeo = new THREE.CylinderGeometry(1.4, 1.5, 0.15, 24);
      const baseMesh = new THREE.Mesh(baseGeo, darkTrimMat);
      baseMesh.position.y = -1.1;
      pGroup.add(baseMesh);

      const glowRingGeo = new THREE.TorusGeometry(1.42, 0.025, 8, 32);
      const glowRingMat = new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.7 });
      const glowRing = new THREE.Mesh(glowRingGeo, glowRingMat);
      glowRing.rotation.x = Math.PI / 2;
      glowRing.position.y = -1.02;
      pGroup.add(glowRing);
      return pGroup;
    };

    NODES.forEach((node) => {
      const group = new THREE.Group();
      group.position.set(...node.position);

      let clickHitMesh: THREE.Mesh;

      // Add Pedestal under node
      group.add(createPedestal(node.color));

      if (node.id === 'kafka') {
        // --- REALISTIC KAFKA 3-TIER PARTITION SERVER RACK TOWER ---
        const towerGroup = new THREE.Group();

        // Main Server Chassis
        const chassisGeo = new THREE.BoxGeometry(1.8, 2.2, 1.4);
        const chassis = new THREE.Mesh(chassisGeo, chassisMat);
        towerGroup.add(chassis);
        clickHitMesh = chassis;

        // 3 Partition Trays (0, 1, 2)
        for (let i = 0; i < 3; i++) {
          const trayY = (i - 1) * 0.62;
          const trayGeo = new THREE.BoxGeometry(1.65, 0.48, 0.1);
          const trayMesh = new THREE.Mesh(trayGeo, brushedAluMat);
          trayMesh.position.set(0, trayY, 0.71);
          towerGroup.add(trayMesh);

          // Handle bar
          const handleGeo = new THREE.CylinderGeometry(0.02, 0.02, 0.8, 8);
          const handleMesh = new THREE.Mesh(handleGeo, darkTrimMat);
          handleMesh.rotation.z = Math.PI / 2;
          handleMesh.position.set(0, trayY, 0.8);
          towerGroup.add(handleMesh);

          // Partition Status LEDs
          for (let l = 0; l < 4; l++) {
            const ledGeo = new THREE.BoxGeometry(0.04, 0.04, 0.02);
            const ledMat = new THREE.MeshBasicMaterial({ color: l === 0 ? 0x10b981 : 0xf59e0b });
            const led = new THREE.Mesh(ledGeo, ledMat);
            led.position.set(-0.6 + l * 0.12, trayY + 0.12, 0.77);
            towerGroup.add(led);
          }
        }

        // Top activity optic strip
        const topBarGeo = new THREE.BoxGeometry(1.6, 0.05, 0.04);
        const topBarMat = new THREE.MeshBasicMaterial({ color: 0xf59e0b });
        const topBar = new THREE.Mesh(topBarGeo, topBarMat);
        topBar.position.set(0, 1.0, 0.72);
        towerGroup.add(topBar);

        group.add(towerGroup);
      } else if (node.id === 'postgres') {
        // --- REALISTIC POSTGRESQL MULTI-TIER ACID STORAGE VAULT ---
        const vaultGroup = new THREE.Group();

        // Central storage spindle
        const spindleGeo = new THREE.CylinderGeometry(0.5, 0.5, 2.2, 24);
        const spindle = new THREE.Mesh(spindleGeo, darkTrimMat);
        vaultGroup.add(spindle);

        // 4 Stacked Aluminum Storage Platters
        for (let p = 0; p < 4; p++) {
          const platterY = -0.75 + p * 0.5;
          const platterGeo = new THREE.CylinderGeometry(1.2, 1.2, 0.16, 32);
          const platter = new THREE.Mesh(platterGeo, brushedAluMat);
          platter.position.y = platterY;
          vaultGroup.add(platter);

          // Spacer ring between platters
          const spacerGeo = new THREE.CylinderGeometry(0.75, 0.75, 0.1, 24);
          const spacer = new THREE.Mesh(spacerGeo, chassisMat);
          spacer.position.y = platterY + 0.13;
          vaultGroup.add(spacer);
        }

        // Hit mesh
        const hitGeo = new THREE.CylinderGeometry(1.25, 1.25, 2.1, 16);
        const hitMat = new THREE.MeshBasicMaterial({ transparent: true, opacity: 0 });
        clickHitMesh = new THREE.Mesh(hitGeo, hitMat);
        vaultGroup.add(clickHitMesh);

        // 4 Outer Titanium Reinforcing Pillars
        for (let col = 0; col < 4; col++) {
          const angle = (col / 4) * Math.PI * 2;
          const pillarGeo = new THREE.CylinderGeometry(0.04, 0.04, 2.0, 12);
          const pillar = new THREE.Mesh(pillarGeo, darkTrimMat);
          pillar.position.set(Math.cos(angle) * 1.35, 0, Math.sin(angle) * 1.35);
          vaultGroup.add(pillar);
        }

        // Fencing Token Aura Ring (Rotating Magnetic Lock)
        const lockRingGeo = new THREE.TorusGeometry(1.45, 0.04, 12, 36);
        const lockRingMat = new THREE.MeshStandardMaterial({
          color: 0x8b5cf6,
          emissive: 0x6d28d9,
          emissiveIntensity: 1.5,
          roughness: 0.1,
        });
        const lockRing = new THREE.Mesh(lockRingGeo, lockRingMat);
        lockRing.rotation.x = Math.PI / 2;
        vaultGroup.add(lockRing);

        group.add(vaultGroup);
      } else if (node.id === 'redis') {
        // --- REALISTIC REDIS L1 HIGH-SPEED IN-MEMORY CORE BLADE ---
        const redisGroup = new THREE.Group();

        // Dark PCB Substrate Blade
        const pcbGeo = new THREE.BoxGeometry(1.8, 1.6, 0.12);
        const pcb = new THREE.Mesh(pcbGeo, chassisMat);
        redisGroup.add(pcb);
        clickHitMesh = pcb;

        // Gold edge connector fingers along bottom
        const goldGeo = new THREE.BoxGeometry(1.7, 0.15, 0.14);
        const goldMat = new THREE.MeshStandardMaterial({ color: 0xd97706, metalness: 0.9, roughness: 0.2 });
        const goldEdge = new THREE.Mesh(goldGeo, goldMat);
        goldEdge.position.y = -0.75;
        redisGroup.add(goldEdge);

        // 4 Fast-Path Silicon Memory Dies
        for (let d = 0; d < 4; d++) {
          const dieX = -0.45 + (d % 2) * 0.9;
          const dieY = 0.35 - Math.floor(d / 2) * 0.65;
          const dieGeo = new THREE.BoxGeometry(0.5, 0.45, 0.08);
          const die = new THREE.Mesh(dieGeo, darkTrimMat);
          die.position.set(dieX, dieY, 0.09);
          redisGroup.add(die);

          // LED trace per die
          const dieLedGeo = new THREE.BoxGeometry(0.04, 0.04, 0.02);
          const dieLedMat = new THREE.MeshBasicMaterial({ color: 0x10b981 });
          const dieLed = new THREE.Mesh(dieLedGeo, dieLedMat);
          dieLed.position.set(dieX + 0.18, dieY + 0.14, 0.14);
          redisGroup.add(dieLed);
        }

        // Translucent Crystalline Heat-Spreader Core
        const crystalGeo = new THREE.BoxGeometry(1.4, 0.5, 0.25);
        const crystalMat = new THREE.MeshStandardMaterial({
          color: 0x10b981,
          emissive: 0x059669,
          emissiveIntensity: 0.6,
          transparent: true,
          opacity: 0.45,
          roughness: 0.1,
          metalness: 0.4,
        });
        const crystal = new THREE.Mesh(crystalGeo, crystalMat);
        crystal.position.set(0, 0, 0.12);
        redisGroup.add(crystal);

        // Sub-3ms Fast-Path Orbital Ring
        const ringGeo = new THREE.RingGeometry(1.3, 1.4, 32);
        const ringMat = new THREE.MeshBasicMaterial({ color: 0x10b981, side: THREE.DoubleSide, transparent: true, opacity: 0.4 });
        const ring = new THREE.Mesh(ringGeo, ringMat);
        ring.rotation.x = Math.PI / 2;
        redisGroup.add(ring);

        group.add(redisGroup);
      } else if (node.id === 'gateway') {
        // --- REALISTIC REST INGESTION GATEWAY APPLIANCE ---
        const gwGroup = new THREE.Group();

        // 1U Rackmount Unit
        const rackGeo = new THREE.BoxGeometry(2.2, 0.75, 1.4);
        const rack = new THREE.Mesh(rackGeo, chassisMat);
        gwGroup.add(rack);
        clickHitMesh = rack;

        // Front Bezel Trim
        const bezelGeo = new THREE.BoxGeometry(2.1, 0.65, 0.06);
        const bezel = new THREE.Mesh(bezelGeo, brushedAluMat);
        bezel.position.set(0, 0, 0.72);
        gwGroup.add(bezel);

        // Optical Fiber SFP Ports (Glowing Cyan)
        for (let p = 0; p < 6; p++) {
          const portGeo = new THREE.BoxGeometry(0.18, 0.14, 0.05);
          const portMat = new THREE.MeshBasicMaterial({ color: 0x38bdf8 });
          const port = new THREE.Mesh(portGeo, portMat);
          port.position.set(-0.75 + p * 0.3, -0.05, 0.76);
          gwGroup.add(port);
        }

        // Status Display Window
        const dispGeo = new THREE.BoxGeometry(0.5, 0.18, 0.03);
        const dispMat = new THREE.MeshBasicMaterial({ color: 0x0284c7 });
        const disp = new THREE.Mesh(dispGeo, dispMat);
        disp.position.set(0.65, 0.15, 0.76);
        gwGroup.add(disp);

        // Dual Fiber Antennas
        for (let a = 0; a < 2; a++) {
          const antGeo = new THREE.CylinderGeometry(0.02, 0.02, 0.9, 8);
          const ant = new THREE.Mesh(antGeo, darkTrimMat);
          ant.position.set(-0.8 + a * 1.6, 0.6, -0.5);
          gwGroup.add(ant);
        }

        group.add(gwGroup);
      } else {
        // --- REALISTIC DLQ QUARANTINE ISOLATION VAULT ---
        const dlqGroup = new THREE.Group();

        // Octagonal Reinforced Containment Chassis
        const octGeo = new THREE.CylinderGeometry(1.0, 1.15, 1.4, 8);
        const oct = new THREE.Mesh(octGeo, chassisMat);
        dlqGroup.add(oct);
        clickHitMesh = oct;

        // Top Containment Hatch Lid
        const hatchGeo = new THREE.CylinderGeometry(0.85, 0.85, 0.2, 8);
        const hatch = new THREE.Mesh(hatchGeo, brushedAluMat);
        hatch.position.y = 0.75;
        dlqGroup.add(hatch);

        // Hazard Warning Ring (Amber/Rose)
        const hazardRingGeo = new THREE.TorusGeometry(1.18, 0.04, 8, 24);
        const hazardRingMat = new THREE.MeshStandardMaterial({
          color: 0xf43f5e,
          emissive: 0xbe123c,
          emissiveIntensity: 1.2,
        });
        const hazardRing = new THREE.Mesh(hazardRingGeo, hazardRingMat);
        hazardRing.rotation.x = Math.PI / 2;
        dlqGroup.add(hazardRing);

        group.add(dlqGroup);
      }

      // Metadata for click raycasting
      clickHitMesh.userData = { nodeData: node };
      interactiveMeshes.push(clickHitMesh);
      scene.add(group);
      nodeMeshes.push(group);
    });

    // 7. High-Precision Fiber-Optic Conduits Connecting Modules
    const connectionPairs = [
      { from: NODES[0].position, to: NODES[1].position, color: 0x38bdf8 }, // Gateway -> Kafka
      { from: NODES[1].position, to: NODES[2].position, color: 0xf59e0b }, // Kafka -> Redis
      { from: NODES[2].position, to: NODES[3].position, color: 0x10b981 }, // Redis -> Postgres
      { from: NODES[1].position, to: NODES[4].position, color: 0xf43f5e }, // Kafka -> DLQ
    ];

    const curves: THREE.QuadraticBezierCurve3[] = [];

    connectionPairs.forEach(pair => {
      const vFrom = new THREE.Vector3(...pair.from);
      const vTo = new THREE.Vector3(...pair.to);
      const mid = new THREE.Vector3().addVectors(vFrom, vTo).multiplyScalar(0.5);
      mid.y += 1.0; // natural arched conduit

      const curve = new THREE.QuadraticBezierCurve3(vFrom, mid, vTo);
      curves.push(curve);

      // Glass cable conduit
      const tubeGeo = new THREE.TubeGeometry(curve, 36, 0.035, 8, false);
      const tubeMat = new THREE.MeshBasicMaterial({ color: pair.color, transparent: true, opacity: 0.35 });
      const tubeMesh = new THREE.Mesh(tubeGeo, tubeMat);
      scene.add(tubeMesh);
    });

    // 8. Stream Energy Packets (Travelling Light Capsules)
    const packets: { mesh: THREE.Mesh; curveIndex: number; progress: number; speed: number }[] = [];
    const packetGeo = new THREE.SphereGeometry(0.09, 12, 12);

    for (let i = 0; i < 10; i++) {
      const cIdx = i % curves.length;
      const color = connectionPairs[cIdx].color;
      const packetMat = new THREE.MeshBasicMaterial({ color });
      const packetMesh = new THREE.Mesh(packetGeo, packetMat);
      scene.add(packetMesh);

      packets.push({
        mesh: packetMesh,
        curveIndex: cIdx,
        progress: Math.random(),
        speed: 0.005 + Math.random() * 0.004,
      });
    }

    // Burst speed trigger
    triggerPulseRef.current = () => {
      packets.forEach(p => {
        p.speed = 0.022;
        setTimeout(() => { p.speed = 0.006; }, 2200);
      });
    };

    // 9. Raycasting Interaction
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

      controls.autoRotate = autoRotate;
      controls.update();

      // Subtle breathing pulse for server racks (firm and realistic, not wildly spinning!)
      nodeMeshes.forEach((group, idx) => {
        group.position.y = NODES[idx].position[1] + Math.sin(elapsedTime * 1.5 + idx) * 0.04;
      });

      // Animate stream packets
      packets.forEach(p => {
        p.progress += p.speed;
        if (p.progress > 1) p.progress = 0;
        const pt = curves[p.curveIndex].getPoint(p.progress);
        p.mesh.position.copy(pt);
      });

      renderer.render(scene, camera);
    };

    animate();

    // 11. Resize
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
    <div className="bg-zinc-900/40 border border-zinc-800/80 rounded-2xl p-4 sm:p-5 shadow-2xl relative overflow-hidden group">
      {/* 3D Header HUD */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-3.5 border-b border-zinc-800/80 gap-3 relative z-10">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm sm:text-base font-semibold text-white flex items-center gap-2">
              <Server className="w-4 h-4 text-zinc-300 shrink-0" />
              <span>3D Distributed Infrastructure & Stream Topology</span>
            </h2>
            <span className="hidden xs:inline-flex px-2 py-0.5 rounded-md text-[10px] font-mono font-medium bg-zinc-800 text-zinc-300 border border-zinc-700/60">
              Interactive Hardware Cluster
            </span>
          </div>
          <p className="text-xs text-zinc-400 mt-0.5">
            Click any server blade to inspect ACID guarantees • Orbit & zoom with mouse / touch
          </p>
        </div>

        {/* 3D Controls Button Group */}
        <div className="flex items-center gap-2 shrink-0">
          <button
            onClick={() => setAutoRotate(!autoRotate)}
            className={`px-2.5 py-1.5 rounded-lg border text-xs font-medium flex items-center gap-1.5 transition ${
              autoRotate 
                ? 'bg-zinc-800 border-zinc-700 text-white shadow-sm' 
                : 'bg-zinc-900 border-zinc-800 text-zinc-400 hover:text-white'
            }`}
          >
            {autoRotate ? <Pause className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
            <span className="hidden xs:inline">{autoRotate ? 'Pause Rotation' : 'Rotate'}</span>
          </button>

          <button
            onClick={handlePulse}
            className="px-3 py-1.5 rounded-lg bg-white hover:bg-zinc-200 text-zinc-950 font-semibold text-xs shadow-sm flex items-center gap-1.5 transition"
          >
            <Sparkles className="w-3.5 h-3.5 fill-current" />
            <span>Burst Pulse</span>
          </button>
        </div>
      </div>

      {/* Main 3D Canvas Box */}
      <div className="relative w-full h-[320px] xs:h-[380px] sm:h-[450px] lg:h-[500px] rounded-xl overflow-hidden bg-zinc-950 border border-zinc-800 mt-3.5 shadow-inner">
        {/* Three.js Container Mount */}
        <div ref={mountRef} className="w-full h-full cursor-grab active:cursor-grabbing touch-none" />

        {/* Floating 3D Legend & Node Stats Overlay */}
        {selectedNode && (
          <motion.div 
            initial={{ opacity: 0, scale: 0.95, y: 8 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            className="absolute top-3 left-3 sm:top-4 sm:left-4 max-w-[240px] xs:max-w-[280px] sm:max-w-xs bg-zinc-950/90 backdrop-blur-xl border border-zinc-800 rounded-xl p-3.5 shadow-2xl font-sans z-20"
          >
            <div className="flex items-center justify-between pb-1.5 mb-1.5 border-b border-zinc-800">
              <span className="text-[10px] font-mono font-medium text-zinc-400 uppercase tracking-wider">
                {selectedNode.category}
              </span>
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
            </div>
            <h3 className="text-xs sm:text-sm font-bold text-white">{selectedNode.name}</h3>
            <p className="text-[11px] sm:text-xs text-zinc-300 mt-1 leading-relaxed line-clamp-3 sm:line-clamp-none">
              {selectedNode.role}
            </p>
            <div className="mt-2.5 pt-1.5 border-t border-zinc-800/80 text-[10px] sm:text-[11px] font-mono text-zinc-400 flex items-center gap-1">
              <Info className="w-3 h-3 text-zinc-400 shrink-0" />
              <span className="truncate">{selectedNode.stats}</span>
            </div>
          </motion.div>
        )}

        {/* 3D Real-Time Telemetry Bar floating in bottom center */}
        <div className="absolute bottom-3 left-3 right-3 sm:bottom-4 sm:left-4 sm:right-4 bg-zinc-950/85 backdrop-blur-xl border border-zinc-800 rounded-xl p-2.5 sm:p-3 flex items-center justify-between gap-2 z-20 text-xs font-mono">
          <div className="flex items-center gap-3 sm:gap-5 overflow-x-auto no-scrollbar">
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-sky-400" />
              <span className="text-zinc-300">Gateway</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-amber-400" />
              <span className="text-zinc-300">Kafka 3x Partitions</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-emerald-400" />
              <span className="text-zinc-300">Redis L1</span>
            </div>
            <div className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-purple-400" />
              <span className="text-zinc-300">PostgreSQL ACID</span>
            </div>
          </div>

          <div className="text-zinc-400 text-[11px] shrink-0 font-sans">
            Pulses: <span className="text-white font-bold font-mono">{pulseCount}</span>
          </div>
        </div>
      </div>
    </div>
  );
};
