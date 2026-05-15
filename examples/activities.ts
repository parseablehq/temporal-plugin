export async function greet(name: string): Promise<string> {
  return `Hello, ${name}!`;
}

export async function chargeCard(_amount: number): Promise<string> {
  throw new Error('payment provider unreachable');
}
