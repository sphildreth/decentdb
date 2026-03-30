using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DecentDb.ShowCase.Entities;

public class AppEventLog
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(50)]
    public string Level { get; set; } = "Info";

    [Required]
    [MaxLength(500)]
    public string Message { get; set; } = string.Empty;

    public DateTime Timestamp { get; set; }

    public string[] Tags { get; set; } = Array.Empty<string>();

    public int[] AffectedIds { get; set; } = Array.Empty<int>();
}
