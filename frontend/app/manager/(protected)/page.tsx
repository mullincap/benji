import { redirect } from "next/navigation";

export default function ManagerRoot() {
  redirect("/manager/overview");
}
