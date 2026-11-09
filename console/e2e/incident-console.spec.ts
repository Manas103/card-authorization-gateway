import { test, expect } from '@playwright/test';

test('opens an incident directly on its triggering segment and dominant decline code', async ({ page }) => {
  await page.goto('/');

  const firstOpenButton = page.locator('[data-testid^="open-incident-"]').first();
  await expect(firstOpenButton).toBeVisible();
  await firstOpenButton.click();

  const segment = page.getByTestId('triggering-segment');
  const declineCode = page.getByTestId('dominant-decline-code');
  await expect(segment).toBeVisible();
  await expect(declineCode).toBeVisible();

  // The segment text must name a real window (a "to" range), not a placeholder,
  // and the decline code must be one of the five this project tracks.
  await expect(segment).toContainText('to');
  await expect(declineCode).toContainText(/INSUFFICIENT_FUNDS|DO_NOT_HONOR|SUSPECTED_FRAUD|INVALID_PIN|EXPIRED_CARD/);
});
