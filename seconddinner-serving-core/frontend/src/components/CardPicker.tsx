interface CardPickerProps {
  cards: string[];
  selected: string | null;
  onSelect: (card: string) => void;
}

export default function CardPicker({ cards, selected, onSelect }: CardPickerProps) {
  return (
    <div className="panel">
      <div className="panel-title">SELECT A CARD</div>
      <div className="flex flex-wrap gap-2">
        {cards.map((card) => (
          <button
            key={card}
            onClick={() => onSelect(card)}
            className={`px-3 py-1.5 rounded text-[10px] font-bold tracking-wider border transition-all duration-150 cursor-pointer
              ${
                selected === card
                  ? "border-cyan bg-cyan/20 text-cyan shadow-neon-cyan"
                  : "border-border text-text-dim hover:border-cyan/50 hover:text-white"
              }`}
          >
            {card.toUpperCase()}
          </button>
        ))}
      </div>
    </div>
  );
}
