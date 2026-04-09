"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function ManagerPublicLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  useEffect(() => {
    router.replace("/compiler/login");
  }, [router]);
  return null;
}
