using System.ComponentModel.DataAnnotations;

namespace DecentDb.ShowCase.Entities;

public class Tag
{
    [Key]
    public int Id { get; set; }

    [Required]
    [MaxLength(50)]
    public string Name { get; set; } = string.Empty;

    [MaxLength(255)]
    public string? Description { get; set; }

    public bool IsSystem { get; set; }

    public int UsageCount { get; set; }

    public DateTime CreatedAt { get; set; }

    public ICollection<ProductTag> ProductTags { get; set; } = new List<ProductTag>();
}
