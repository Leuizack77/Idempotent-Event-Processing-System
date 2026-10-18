import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  cacheComponents: true,
  partialPrefetching: true,
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
  async rewrites() {
    return [
      {
        source: "/api/backend/:path*",
        destination: "http://localhost:8080/api/:path*",
      },
      {
        source: "/actuator/:path*",
        destination: "http://localhost:8080/actuator/:path*",
      },
    ];
  },
};

export default nextConfig;
