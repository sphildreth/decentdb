using System.ComponentModel.DataAnnotations;
using NodaTime;

namespace DecentDb.ShowCase.Entities;

public class ScheduleEntry
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(200)]
    public string Title { get; set; } = string.Empty;

    public Instant ScheduledInstant { get; set; }

    public LocalDate ScheduledDate { get; set; }

    public LocalDateTime ScheduledLocalDateTime { get; set; }

    public LocalDate EffectiveFrom { get; set; }

    public LocalDate EffectiveUntil { get; set; }

    public bool IsCompleted { get; set; }

    public int Priority { get; set; }

    public DateTime CreatedAt { get; set; }

    public DateTime? CompletedAt { get; set; }
}
