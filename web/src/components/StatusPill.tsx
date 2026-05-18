type StatusPillProps = {
  active: boolean;
  activeLabel: string;
  inactiveLabel: string;
};

export default function StatusPill({
  active,
  activeLabel,
  inactiveLabel,
}: StatusPillProps) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-bold ${
        active ? "bg-green-50 text-green-700" : "bg-red-50 text-red-600"
      }`}
    >
      <span
        className={`mr-2 h-2 w-2 rounded-full ${
          active ? "bg-green-500" : "bg-red-500"
        }`}
      />
      {active ? activeLabel : inactiveLabel}
    </span>
  );
}
