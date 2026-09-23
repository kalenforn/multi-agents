import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
  title: "Multi-Agent Workbench",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="zh-CN">
      <body style={{ margin: 0, fontFamily: "system-ui, -apple-system, sans-serif", background: "#f2f4f3", color: "#17232a" }}>
        {children}
      </body>
    </html>
  );
}
