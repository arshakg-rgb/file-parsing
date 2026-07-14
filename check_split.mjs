const line = '22015694831|Мансурова Махирахон|21.09.1985|||||7804610785||';
console.log('line.length:', line.length);
for (const delim of [',', ';', '\\t', '|']) {
  const parts = line.split(delim);
  console.log(`delim '${delim}' parts.length:`, parts.length, 'parts[0]:', parts[0].slice(0, 30));
}
