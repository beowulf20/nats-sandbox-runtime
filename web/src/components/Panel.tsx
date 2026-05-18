import type { ReactNode } from "react";

type PanelProps = {
  children: ReactNode;
  className?: string;
};

export default function Panel({ children, className = "" }: PanelProps) {
  return (
    <section
      className={`rounded-lg border border-gray-200 bg-white p-5 shadow-sm shadow-gray-200/70 ${className}`}
    >
      {children}
    </section>
  );
}
