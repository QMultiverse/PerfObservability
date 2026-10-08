# Fills in the guide's contents page with Word's own page numbers.
#
# The contents list is a Word field; docx-js cannot compute page numbers, and
# letting Word refresh it on open gave "page 1" for every entry, because the
# refresh ran before Word had laid out the pages. This opens the document in a
# hidden Word, lays it out, updates the list and saves.
#
# Usage:  powershell -File update_toc.ps1 <path-to-docx>
param([Parameter(Mandatory = $true)][string]$Path)

$full = (Resolve-Path $Path).Path
try { $word = New-Object -ComObject Word.Application }
catch {
  Write-Warning "Microsoft Word is not available. Open the document, right-click the contents list and choose Update Field, Update entire table."
  exit 0
}
$word.Visible = $false
$word.DisplayAlerts = 0
try {
  $doc = $word.Documents.Open($full, $false, $false, $false)
  # Lay out, update, then again: the list itself can push pages along.
  $doc.Repaginate()
  foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
  $doc.Repaginate()
  foreach ($toc in $doc.TablesOfContents) { $toc.UpdatePageNumbers() }
  $doc.Save()
  $doc.Close()
  Write-Output "contents updated: $full"
}
finally { $word.Quit() }
